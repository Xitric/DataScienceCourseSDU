import findspark
from geo_pyspark.utils import GeoSparkKryoRegistrator, KryoSerializer
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, lit

from geo_pyspark.register import upload_jars
from geo_pyspark.register import GeoSparkRegistrator

from incident_modern_context import IncidentModernContext

DATA_SOURCE_FORMAT = "org.apache.spark.sql.execution.datasources.hbase"
NEIGHBORHOODS = ["Seacliff", "Lake Street", "Presidio National Park", "Presidio Terrace", "Inner Richmond",
                 "Sutro Heights", "Lincoln Park / Ft. Miley", "Outer Richmond", "Golden Gate Park", "Presidio Heights",
                 "Laurel Heights / Jordan Park", "Lone Mountain", "Anza Vista", "Cow Hollow", "Union Street", "Nob Hill",
                 "Laurel Heights / Jordan Park", "Lone Mountain", "Anza Vista", "Cow Hollow", "Union Street",
                 "Nob Hill",
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
                 "Portola", "University Mound", "St. Mary's Park", "Mission Terrace", "Sunnyside", "Glen Park",
                 "Western Addition", "Aquatic Park / Ft. Mason", "Fisherman's Wharf", "Cathedral Hill", "Japantown",
                 "Pacific Heights", "Lower Pacific Heights", "Chinatown", "Polk Gulch", "North Beach", "Russian Hill",
                 "Financial District", "Inner Sunset", "Parnassus Heights", "Forest Knolls", "Buena Vista",
                 "Corona Heights", "Ashbury Heights", "Eureka Valley", "St. Francis Wood", "Sherwood Forest"]
CATEGORIES = ["Street and Sidewalk Cleaning", "Graffiti", "Abandoned Vehicle", "Encampments", "MUNI Feedback",
              "Parking Enforcement", "Damaged Property", "Sewer Issues", "General Request - PUBLIC WORKS",
              "Tree Maintenance", "General Request - MTA", "Streetlights", "SFHA Requests", "Street Defects",
              "Litter Receptacles", "Illegal Postings", "Rec and Park Requests", "Sign Repair", "Sidewalk or Curb",
              "Blocked Street or SideWalk", "Homeless Concerns", "General Request - PUC", "Temporary Sign Request",
              "General Request - DPH", "Noise Report", "311 External Request", "General Request - COUNTY CLERK",
              "Color Curb", "General Request - 311CUSTOMERSERVICECENTER", "Residential Building Request",
              "Construction Zone Permits", "General Request - ASSESSOR RECORDER",
              "General Request - BUILDING INSPECTION", "Catch Basin Maintenance",
              "General Request - ANIMAL CARE CONTROL", "KB Events", "General Request - CITY ATTORNEY",
              "General Request - RPD", "General Request - FIRE DEPARTMENT",
              "General Request - MONS", "DPW Volunteer Programs", "General Request - DTIS", "General Request - SFPD",
              "General Request - DISTRICT ATTORNEY", "General Request - PLANNING", "General Request - PORT AUTHORITY",
              "General Request - TT COLLECTOR", "General Request - HUMAN SERVICES AGENCY",
              "General Request - BOARD OF SUPERVISORS", "General Request - ENTERTAINMENT COMMISSION",
              "General Request - DPT", "General Request - HUMAN RESOURCES", "General Request - ART COMMISSION",
              "General Request - TAXI COMMISSION", "General Request - ENVIRONMENT", "General Request - RENT BOARD",
              "General Request -", "General Request - CITYADMINISTRATOR GSA", "General Request - MOD",
              "General Request - CITY HALL", "General Request - MOCD", "General Request - OCC",
              "General Request - ELECTIONS", "General Request - HSH", "General Request - LABOR STANDARDS ENFORCEMENT",
              "General Request - SMALL BUSINESS", "General Request - LIBRARY PUBLIC", "General Request - SHERIFF",
              "General Request - MOH", "General Request - CONTROLLER", "General Request - CENTRAL SHOPS",
              "General Request - REAL ESTATE DEPARTMENT", "General Request - AGING ADULT SERVICES",
              "General Request - CONTRACT ADMINISTRATION", "General Request - HUMAN RIGHTS COMMISSION",
              "General Request - FILM COMMISSION", "General Request - ECONOMICS AND WORKFORCE DEVELOPMENT",
              "General Request - CHILDSUPPORTSERVICES", "General Request - MOCJ", "General Request - TIDA",
              "General Request - AIRPORT SFO", "General Request - MEDICAL EXAMINER",
              "General Request - SHORT TERM RENTALS", "General Request - CHILDREN YOUTH FAMILIES",
              "General Request - PUBLIC DEFENDER", "General Request - ADULT PROBATION", "General Request - DEM",
              "General Request - ETHICS COMMISSION", "General Request - AGINGADULTSERVICES",
              "General Request - STATUS OF WOMEN", "General Request - WAR MEMORIAL", "General Request - CITY COLLEGE",
              "General Request - MUNI", "General Request - CONVENTION FACILITIES",
              "General Request - LANGUAGE SERVICES", "General Request - RISK MANAGEMENT",
              "General Request - GRANTS FOR THE ARTS", "General Request - JUVENILE PROBATION",
              "General Request - BOARD OF APPEALS", "General Request - MUSEUMS",
              "General Request - REDEVELOPMENT AGENCY", "PUC Sewer Ops"]

if __name__ == "__main__":
    # SparkContext is old tech! Therefore we use the modern SparkSession
    upload_jars()

    spark = SparkSession.builder \
        .master("local") \
        .config('spark.driver.host', '127.0.0.1') \
        .config("spark.jars", "/backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.network.timeout", "60s") \
        .appName("SfDataImporter") \
        .getOrCreate()

    # .config("spark.serializer", KryoSerializer.getName) \
    # .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \

   # GeoSparkRegistrator.registerAll(spark)

   # GeoSparkRegistrator.register(SparkSession.builder.getOrCreate())



    #context = ServiceCaseContext(spark)
    # df = context.load_csv() \
    #     .limit(150000)  # For testing purposes

    # context.save_hbase(df)
    #load_df = context.load_hbase()


    # schema = load_df.schema

    # @pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
    # def count_graffiti(df: pandas.DataFrame):
    #     neighborhood = df["neighborhood_id"].iloc[0]
    #     count = df.where((df["category_id"] == CATEGORIES.index("Graffiti"))).count()
    #     return pandas.DataFrame([neighborhood] + [count])

    # load_df.show(100, False)
    # load_df.where((load_df["category_id"] == (hash(CATEGORIES[0]) & 0xffffffff))) \
    #     .groupBy(load_df["neighborhood_id"]) \
    #     .agg(count(lit(1)).alias("sidewalk_cleaning")) \
    #     .show(200, False)

    # load_df.where((load_df["neighborhood_id"] < 5) & (load_df["category_id"] == CATEGORIES.index("Graffiti"))) \
    #     .groupBy(load_df["neighborhood_id"]) \
    #     .apply(count_graffiti) \
    #     .show(10, False)
    # print(load_df.count())

    context = IncidentModernContext(spark)
    context.load_csv()#.show(200, true)

