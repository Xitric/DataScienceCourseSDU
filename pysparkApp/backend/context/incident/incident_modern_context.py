import json
import os

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, unix_timestamp, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, Row
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.flume import FlumeUtils

from context.context import Context
from util.neighborhood_boundaries import add_neighborhoods
from util.string_hasher import string_hash

hasher = udf(
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
            "rowkey":"key_neighborhood:key_category:key_datetime:key_row_id",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "category_id":{"cf":"rowkey", "col":"key_category", "type":"int"},
                "opened":{"cf":"rowkey", "col":"key_datetime", "type":"int"},
                "row_id":{"cf":"rowkey", "col":"key_row_id", "type":"int"},
                
                "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},
                "category":{"cf":"a", "col":"category", "type":"string"},
                "subcategory":{"cf":"a", "col":"subcategory", "type":"string"},
                "opened":{"cf":"a", "col":"opened", "type":"int"},
                "report_datetime":{"cf":"a", "col":"report_datetime", "type":"int"},
                "id":{"cf":"a", "col":"id", "type":"string"},
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
            .load(self.__incident_modern_file)

        # Remove rows missing category or location information
        incidents_df = incidents_df.where(
            incidents_df["Incident Category"].isNotNull() &
            incidents_df["Latitude"].isNotNull() &
            incidents_df["Longitude"].isNotNull()
        )

        # Select the relevant columns
        incidents_df = incidents_df.select(
            hasher("Row ID").alias("row_id"),
            incidents_df["Incident Category"].alias("category"),
            hasher("Incident Category").alias("category_id"),
            incidents_df["Incident Subcategory"].alias("subcategory"),
            unix_timestamp("Incident Datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType()).alias("opened"),
            unix_timestamp("Report Datetime", "yyyy/MM/dd hh:mm:ss a").cast(IntegerType()).alias("report_datetime"),
            incidents_df["Incident ID"].alias("id"),
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

        # Add neighborhood column
        incidents_df = add_neighborhoods(incidents_df)
        incidents_df = incidents_df.withColumn("neighborhood_id", hasher(incidents_df["neighborhood"]))

        return incidents_df

    def load_flume(self, ssc: StreamingContext) -> DStream:
        # stream that pulls inputs from Flume
        # maybe change host name
        print("LOADING FLUME")
        input_stream = FlumeUtils.createStream(ssc, self.__flume_host, self.__flume_port)
        d_stream = input_stream.map(self.__parse_json).transform(lambda rdd: self.__convert_service_format(rdd))
        return d_stream

    @staticmethod
    def __parse_json(data: str) -> Row:
        # Read the json data as a dict
        data_dict = json.loads(data[1])
        # Make a Row object from the data
        row = Row(
            row_id=data_dict.get("row_id", ""),
            category=data_dict.get("incident_category", ""),
            subcategory=data_dict.get("incident_subcategory", ""),
            opened=data_dict.get("incident_datetime", ""),
            report_datetime=data_dict.get("report_datetime", ""),
            id=data_dict.get("incident_id", ""),
            resolution=data_dict.get("resolution", ""),
            intersection=data_dict.get("intersection", ""),
            latitude=float(data_dict.get("latitude", "0")),
            longitude=float(data_dict.get("longitude", "0")),
            report_type_code=data_dict.get("report_type_code", ""),
            report_type_description=data_dict.get("report_type_description", ""),
            incident_number=data_dict.get("incident_number", ""),
            police_district=data_dict.get("police_district", ""),
            supervisor_district=data_dict.get("supervisor_district", ""),
        )
        return row

    @staticmethod
    def __convert_service_format(rdd: RDD) -> RDD:
        if rdd.isEmpty():
            return rdd

        df = rdd.toDF()

        df = add_neighborhoods(df)

        df = df.withColumn("row_id", hasher(df["row_id"])) \
            .withColumn("category_id", hasher(df["category"])) \
            .withColumn("opened",
                        unix_timestamp(to_timestamp("opened", "yyyy-MM-dd'T'HH:mm:ss.SSS")).cast(
                            IntegerType())) \
            .withColumn("report_datetime",
                        unix_timestamp(to_timestamp("report_datetime", "yyyy-MM-dd'T'HH:mm:ss.SSS")).cast(
                            IntegerType())) \
            .withColumn("neighborhood_id", hasher(df["neighborhood"]))

        return df.rdd

    def load_hbase(self, session: SparkSession) -> DataFrame:
        return session.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
