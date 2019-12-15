import os

from geo_pyspark.sql.types import GeometryType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, unix_timestamp, to_timestamp
from pyspark.sql.types import BooleanType, DoubleType, IntegerType
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from string_hasher import string_hash
from context import Context


# Create and return an array of points based on the neighborhood boundary string
def create_polygon(neighborhood_boundary_string):
    neighborhood_boundary = neighborhood_boundary_string[16:-3]
    points_with_spaces = neighborhood_boundary.split(", ")
    points = []
    for point in points_with_spaces:
        coordinates = point.split(" ")
        points.append(Point(float(coordinates[1]), float(coordinates[0])))
    return Polygon([[p.x, p.y] for p in points])


polygon = udf(
    lambda neighborhood_boundary_string: create_polygon(neighborhood_boundary_string), GeometryType()
)

# Check if the latitude and longitude is in the neighborhood represented as a Polygon
check_neighborhood_in_polygon = udf(
    lambda latitude, longitude, polygon_object:
    polygon_object.contains(Point(float(latitude), float(longitude))),
    BooleanType()
)

string_to_hash = udf(
    lambda string: string_hash(string),
    IntegerType()
)


class IncidentModernContext(Context):
    # File from HDFS
    incident_modern_file = os.environ["CORE_CONF_fs_defaultFS"] \
                           + "/datasets/Police_Department_Incident_Reports__2018_to_Present.csv"

    __catalog = ''.join("""{
            "table":{"namespace":"default", "name":"modern_incident_reports"},
            "rowkey":"key_analysis_neighborhood:key_incident_category:key_incident_datetime:key_row_id",
            "columns":{
                "analysis_neighborhood_id":{"cf":"rowkey", "col":"key_analysis_neighborhood", "type":"int"},
                "incident_category_id":{"cf":"rowkey", "col":"key_incident_category", "type":"int"},
                "incident_datetime":{"cf":"rowkey", "col":"key_incident_datetime", "type":"int"},
                "row_id":{"cf":"rowkey", "col":"key_row_id", "type":"int"},
                
                "analysis_neighborhood":{"cf":"a", "col":"analysis_neighborhood", "type":"string"},
                "incident_category":{"cf":"a", "col":"incident_category", "type":"string"},
                "incident_subcategory":{"cf":"a", "col":"incident_subcategory", "type":"string"},
                "incident_datetime":{"cf":"a", "col":"incident_datetime", "type":"int"},
                "report_datetime":{"cf":"a", "col":"report_datetime", "type":"int"},
                "incident_id":{"cf":"a", "col":"incident_id", "type":"string"},
                "resolution":{"cf":"a", "col":"resolution", "type":"string"},
                
                "intersection":{"cf":"l", "col":"intersection", "type":"string"},
                "latitude":{"cf":"l", "col":"latitude", "type":"double"},
                "longitude":{"cf":"l", "col":"longitude", "type":"double"},
                
                "report_type_code":{"cf":"m", "col":"report_type_code", "type":"string"},
                "report_type_description":{"cf":"m", "col":"report_type_description", "type":"string"},
                "incident_number":{"cf":"m", "col":"incident_number", "type":"string"},
                "police_district":{"cf":"m", "col":"police_district", "type":"string"},
                "supervisor_district":{"cf":"m", "col":"supervisor_district", "type":"string"}
              }  
    }""".split())

    def load_csv(self, session: SparkSession) -> DataFrame:
        # Read csv file
        incidents_df = session.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.incident_modern_file) \
            .limit(50)

        # Remove rows missing category or location information
        incidents_df = incidents_df.where(
            incidents_df["Incident Category"].isNotNull() &
            incidents_df["Latitude"].isNotNull() &
            incidents_df["Longitude"].isNotNull()
        )

        # Read the neighborhood file from HDFS and create polygons based on the neighborhood boundaries
        sf_boundaries_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/SFFind_Neighborhoods.csv"
        sf_boundaries_df = session.read.format("csv").option("header", "true").load(sf_boundaries_file)
        sf_boundaries_df = sf_boundaries_df.withColumn("the_geom", polygon(sf_boundaries_df["the_geom"]))
        sf_boundaries_df = sf_boundaries_df.select(
            sf_boundaries_df["the_geom"].alias("polygon"),
            sf_boundaries_df["name"].alias("analysis_neighborhood")
        )

        # Select the relevant columns
        incidents_df = incidents_df.select(
            string_to_hash("Row ID").alias("row_id"),
            incidents_df["Incident Category"].alias("incident_category"),
            string_to_hash("Incident Category").alias("incident_category_id"),
            incidents_df["Incident Subcategory"].alias("incident_subcategory"),
            unix_timestamp("Incident Datetime", "yyyy/MM/dd hh:mm:ss a").alias("incident_datetime"),
            unix_timestamp("Report Datetime", "yyyy/MM/dd hh:mm:ss a").alias("report_datetime"),
            incidents_df["Incident ID"].alias("incident_id"),
            incidents_df["Resolution"].alias("resolution"),
            incidents_df["Intersection"].alias("intersection"),
            incidents_df["Latitude"].cast(DoubleType()).alias("latitude"),
            incidents_df["Longitude"].cast(DoubleType()).alias("longitude"),
            incidents_df["Report Type Code"].alias("report_type_code"),
            incidents_df["Report Type Description"].alias("report_type_description"),
            incidents_df["Incident Number"].alias("incident_number"),
            incidents_df["Police District"].alias("police_district"),
            incidents_df["Supervisor District"].alias("supervisor_district")
        )

        # Join incident_modern_df and sf_boundaries_df if latitude and longitude is in the polygon
        incidents_df = incidents_df.join(
            sf_boundaries_df,
            check_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        incidents_df = incidents_df.drop("polygon")
        incidents_df = incidents_df.withColumn("analysis_neighborhood_id", string_to_hash(
            incidents_df["analysis_neighborhood"]))

        incidents_df.show(100, True)
        return incidents_df

    def load_hbase(self, session: SparkSession) -> DataFrame:
        return session.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
