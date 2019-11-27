import os

from pyspark.sql import SparkSession
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from pyspark.sql.functions import to_timestamp, unix_timestamp, udf
from pyspark.sql.types import IntegerType, StringType, BooleanType, ArrayType, FloatType

DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"

NEIGHBORHOODS = ["Seacliff", "Lake Street", "Presidio National Park", "Presidio Terrace", "Inner Richmond",
                 "Sutro Heights", "Lincoln Park / Ft. Miley", "Outer Richmond", "Golden Gate Park", "Presidio Heights",
                 "Laurel Heights / Jordan Par", "Lone Mountain", "Anza Vista", "Cow Hollow", "Union Street", "Nob Hill",
                 "Marina", "Telegraph Hill", "Downtown / Union Square", "Tenderloin", "Civic Center", "Hayes Valley",
                 "Alamo Square", "Panhandle", "Haight Ashbury", "Lower Haight", "Mint Hill", "Duboce Triangle",
                 "Cole Valley", "Rincon Hill", "South Beach", "South of Market", "Showplace Square", "Mission Bay",
                 "Yerba Buena Island", "Treasure Island", "Mission Dolores", "Castro", "Outer Sunset", "Parkside",
                 "Stonestown", "Parkmerced", "Lakeshore", "Golden Gate Heights", "Forest Hill", "West Portal",
                 "Clarendon Heights", "Midtown Terrace", "Laguna Honda", "Lower Nob Hill", "Upper Market",
                 "Dolores Heights", "Mission", "Potrero Hill", "Dogpatch", "Central Waterfront", "Diamond Heights",
                 "Crocker Amazon", "Fairmount", "Peralta Heights", "Holly Park", "Merced Manor", "Balboa Terrace",
                 "Ingleside", "Merced Heights", "Outer Mission", "Ingleside Terraces", "Mt. Davidson Manor",
                 "Monterey Heights", "Westwood Highlands", "Westwood Park", "Miraloma Park", "McLaren Park",
                 "Sunnydale", "Visitacion Valley", "India Basin", "Northern Waterfront", "Hunters Point",
                 "Candlestick Point SRA", "Cayuga", "Oceanview", "Apparel City", "Bernal Heights", "Noe Valley",
                 "Produce Market", "Bayview", "Silver Terrace", "Bret Harte", "Little Hollywood", "Excelsior",
                 "Portola", "University Mound", "St. Marys Park", "Mission Terrace", "Sunnyside", "Glen Park",
                 "Western Addition", "Aquatic Park / Ft. Mason", "Fishermans Wharf", "Cathedral Hill", "Japantown",
                 "Pacific Heights", "Lower Pacific Heights", "Chinatown", "Polk Gulch", "North Beach", "Russian Hill",
                 "Financial District", "Inner Sunset", "Parnassus Heights", "Forest Knolls", "Buena Vista",
                 "Corona Heights", "Ashbury Heights", "Eureka Valley", "St. Francis Wood", "Sherwood Forest"]

if __name__ == "__main__":
    # SparkContext is old tech! Therefore we use the modern SparkSession
    spark = SparkSession.builder \
        .master("local") \
        .config('spark.driver.host', '127.0.0.1') \
        .config("spark.jars", "/backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "60s") \
        .appName("SfDataImporter") \
        .getOrCreate()

    # Read csv file
    # The multiline config is necessary to support strings with line breaks in the csv file
    incident_modern_file = os.environ[
                               "CORE_CONF_fs_defaultFS"] + "/datasets/Police_Department_Incident_Reports__2018_to_Present.csv"
    incident_modern_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("multiline", "true") \
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
        .load(incident_modern_file) \
        .limit(10)  # For testing purposes

    # Remove rows missing category or location information
    incident_modern_df = incident_modern_df.where(
        incident_modern_df["Incident Category"].isNotNull() & incident_modern_df["Analysis Neighborhood"].isNotNull())


    def create_polygon(multipolygon_string):
        multipolygon_clean = multipolygon_string[16:-3]
        points_with_spaces = multipolygon_clean.split(", ")
        points = []
        for point in points_with_spaces:
            coordinates = point.split(" ")
            points.append(Point(float(coordinates[1]), float(coordinates[0])))
        return [[p.x, p.y] for p in points]


    multipolygon = udf(
        create_polygon, ArrayType(ArrayType(FloatType()))
    )

    sf_boundaries_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/SFFind_Neighborhoods.csv"
    sf_boundaries_df = spark.read.format("csv").option("header", "true").load(sf_boundaries_file)
    sf_boundaries_df = sf_boundaries_df.select("the_geom", "name")
    sf_boundaries_df = sf_boundaries_df.withColumn("the_geom", multipolygon(
        sf_boundaries_df["the_geom"],
    ))

    check_neighborhood_in_polygon = udf(
        lambda latitude, longitude, polygon: Polygon(polygon).contains(Point(float(latitude), float(longitude))),
        BooleanType()
    )

    incident_modern_df = incident_modern_df.join(
        sf_boundaries_df,
        check_neighborhood_in_polygon("Latitude", "Longitude", "the_geom"),
        "cross"
    ).select("name", "latitude", "longitude")

    incident_modern_df.show(200, True)
