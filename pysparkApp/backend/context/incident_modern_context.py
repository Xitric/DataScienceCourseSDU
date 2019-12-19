import json
import os

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, unix_timestamp
from pyspark.sql.types import DoubleType, IntegerType, Row
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.flume import FlumeUtils

from context.context import Context
from util.neighborhood_boundaries import neighborhood_boundaries, is_neighborhood_in_polygon
from util.spark_session_utils import get_spark_session_instance
from util.string_hasher import string_hash

string_to_hash = udf(
    lambda string: string_hash(string),
    IntegerType()
)


class IncidentModernContext(Context):
    # File from HDFS
    __incident_modern_file = os.environ["CORE_CONF_fs_defaultFS"] \
                             + "/datasets/Police_Department_Incident_Reports__2018_to_Present.csv"
    __flume_host = "livy"
    __flume_port = 4001

    __catalog = ''.join("""{
            "table":{"namespace":"default", "name":"modern_incident_reports"},
            "rowkey":"key_neighborhood:key_incident_category:key_incident_datetime:key_row_id",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "incident_category_id":{"cf":"rowkey", "col":"key_incident_category", "type":"int"},
                "incident_datetime":{"cf":"rowkey", "col":"key_incident_datetime", "type":"int"},
                "row_id":{"cf":"rowkey", "col":"key_row_id", "type":"int"},
                
                "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},
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

    def load_csv(self, spark: SparkSession) -> DataFrame:
        # Read csv file
        incidents_df = spark.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.__incident_modern_file) \
            .limit(10)

        # Remove rows missing category or location information
        incidents_df = incidents_df.where(
            incidents_df["Incident Category"].isNotNull() &
            incidents_df["Latitude"].isNotNull() &
            incidents_df["Longitude"].isNotNull()
        )

        # Select the relevant columns
        incidents_df = incidents_df.select(
            string_to_hash("Row ID").alias("row_id"),
            incidents_df["Incident Category"].alias("incident_category"),
            string_to_hash("Incident Category").alias("incident_category_id"),
            incidents_df["Incident Subcategory"].alias("incident_subcategory"),
            unix_timestamp("Incident Datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType()).alias("incident_datetime"),
            unix_timestamp("Report Datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType()).alias("report_datetime"),
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

        neighborhood_boundaries_df = neighborhood_boundaries(spark)

        # Join incident_modern_df and neighborhood_boundaries_df if latitude and longitude is in the polygon
        incidents_df = incidents_df.join(
            neighborhood_boundaries_df,
            is_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        incidents_df = incidents_df.drop("polygon")
        incidents_df = incidents_df.withColumn("neighborhood_id", string_to_hash(incidents_df["neighborhood"]))

        return incidents_df

    def load_flume(self, ssc: StreamingContext) -> DStream:
        # stream that pulls inputs from Flume
        # maybe change host name
        print("hej med dig")
        input_stream = FlumeUtils.createStream(ssc, self.__flume_host, self.__flume_port)
        d_stream = input_stream.map(self.__parse_json).transform(lambda rdd: self.__convert_service_format(rdd))
        return d_stream

    @staticmethod
    def __parse_json(data: str) -> Row:
        # Read the json data as a dict
        data_dict = json.loads(data[1])
        # Make a Row object from the data
        print("THIS IS THE JSON DATA:")
        print(data_dict)
        row = Row(
            row_id=data_dict.get("row_id", ""),
            incident_category=data_dict.get("incident_category", ""),
            incident_subcategory=data_dict.get("incident_subcategory", ""),
            incident_datetime=data_dict.get("incident_datetime", ""),
            report_datetime=data_dict.get("report_datetime", ""),
            incident_id=data_dict.get("incident_id", ""),
            resolution=data_dict.get("resolution", ""),
            intersection=data_dict.get("intersection", ""),
            latitude=data_dict.get("latitude", ""),
            longitude=data_dict.get("longitude", ""),
            report_type_code=data_dict.get("report_type_code", ""),
            report_type_description=data_dict.get("report_type_description", ""),
            incident_number=data_dict.get("incident_number", ""),
            police_district=data_dict.get("police_district", ""),
            supervisor_district=data_dict.get("supervisor_district", ""),
        )
        print("THIS IS THE ROW")
        print(row)
        return row

    @staticmethod
    def __convert_service_format(rdd: RDD) -> RDD:
        if rdd.isEmpty():
            return rdd
        df = rdd.toDF()

        spark = get_spark_session_instance(rdd.context.getConf())
        neighborhood_boundaries_df = neighborhood_boundaries(spark)

        df = df.join(
            neighborhood_boundaries_df,
            is_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        df = df.drop("polygon")

        df = df \
            .withColumn("row_id", string_to_hash(df["row_id"])) \
            .withColumn("incident_category_id", string_to_hash(df["incident_category"])) \
            .withColumn("incident_datetime",
                        unix_timestamp("incident_datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType())) \
            .withColumn("report_datetime",
                        unix_timestamp("report_datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType())) \
            .withColumn("neighborhood_id", string_to_hash(df["neighborhood"]))
        return df.rdd

    def load_hbase(self, session: SparkSession) -> DataFrame:
        return session.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
