import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, unix_timestamp, to_timestamp
from pyspark.sql.types import BooleanType, ArrayType, FloatType, DoubleType
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from context import Context


class IncidentModernContext(Context):
    # File from HDFS
    incident_modern_file = os.environ["CORE_CONF_fs_defaultFS"] \
                           + "/datasets/Police_Department_Incident_Reports__2018_to_Present.csv"

    # TODO HBase table
    __catalog = ''.join("""{
            "table":{"namespace:"default", "name":incidents_modern"},
            "rowkey":"key_analysis_neighborhood:key_incident_category:key_incident_datetime:key_row_id",
            "columns":{
                "analysis_neighborhood"
    }""")

    def __init__(self, session: SparkSession):
        super().__init__(session)

    def load_csv(self) -> DataFrame:
        # Read csv file
        # The multiline config is necessary to support strings with line breaks in the csv file
        # TODO FILE
        incidents_modern_df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.incident_modern_file) \
            .limit(10)  # For testing purposes        # TODO file load

        # Remove rows missing category or location information
        incidents_modern_df = incidents_modern_df.where(
            incidents_modern_df["Incident Category"].isNotNull() &
            incidents_modern_df["Latitude"].isNotNull() &
            incidents_modern_df["Longitude"].isNotNull()
        )

        # Create and return an array of points based on the_geom string
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

        # Check if the latitude and longitude is in the neighborhood represented as a Polygon
        check_neighborhood_in_polygon = udf(
            lambda latitude, longitude, neighborhood_boundary:
            Polygon(neighborhood_boundary).contains(Point(float(latitude), float(longitude))),
            BooleanType()
        )

        # Read the neighborhood file and make a polygon based on the_geom column
        sf_boundaries_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/SFFind_Neighborhoods.csv"
        sf_boundaries_df = self.spark.read.format("csv").option("header", "true").load(sf_boundaries_file)
        sf_boundaries_df = sf_boundaries_df.withColumn("the_geom", multipolygon(sf_boundaries_df["the_geom"]))
        sf_boundaries_df = sf_boundaries_df.select(
            sf_boundaries_df["the_geom"].alias("polygon"),
            sf_boundaries_df["name"].alias("analysis_neighborhood")
        )
        sf_boundaries_df.show(200, True)

        incidents_modern_df = incidents_modern_df.select(
            unix_timestamp(to_timestamp("Incident Datetime", "yyyy/MM/dd hh:mm:ss a")).alias("incident_datetime"),
            unix_timestamp(to_timestamp("Report Datetime", "yyyy/MM/dd hh:mm:ss a")).alias("report_datetime"),
            incidents_modern_df["Row ID"].alias("row_id"),
            incidents_modern_df["Incident ID"].alias("incident_id"),  # Docs: Plain text, seems to contain numbers only
            incidents_modern_df["Incident Number"].alias("incident_number"),  # Text, seems to contain numbers only
            incidents_modern_df["Report Type Code"].alias("report_type_code"),
            incidents_modern_df["Report Type Description"].alias("report_type_description"),
            incidents_modern_df["Incident Category"].alias("incident_category"),
            incidents_modern_df["Incident Subcategory"].alias("incident_subcategory"),
            incidents_modern_df["Resolution"].alias("resolution"),
            incidents_modern_df["Intersection"].alias("intersection"),
            incidents_modern_df["Police District"].alias("police_district"),  # Text, seems to contain numbers only
            incidents_modern_df["Supervisor District"].alias("supervisor_district"),
            incidents_modern_df["Latitude"].cast(DoubleType()).alias("latitude"),
            incidents_modern_df["Longitude"].cast(DoubleType()).alias("longitude")
        )

        incidents_modern_df = incidents_modern_df.join(
            sf_boundaries_df,
            check_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        incidents_modern_df.show(200, True)
        return incidents_modern_df


# TODO IMPLEMENT CONTEXT
def load_hbase(self) -> DataFrame:
    return self.spark.read.option()
    pass


def save_hbase(self, df: DataFrame):
    pass
