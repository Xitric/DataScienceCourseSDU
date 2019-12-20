import json
import os

from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import udf, unix_timestamp, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.flume import FlumeUtils

from context.context import Context
from util.neighborhood_boundaries import neighborhood_boundaries, is_neighborhood_in_polygon
from util.spark_session_utils import get_spark_session_instance
from util.string_hasher import string_hash

# User defined function for calculating category and neighborhood numbers
hasher = udf(
    lambda category_str: string_hash(category_str),  # Conversion function
    IntegerType()  # Return type
)


class ServiceCaseContext(Context):
    """A class for easily handling various sources of 311 service cases, and ensuring that they are handled in a
    common format"""

    # The file to read from HDFS
    __file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/311_Cases.csv"

    # The host to which Flume ingests data
    __flume_host = "livy"

    # The port at which Flume ingests service cases
    __flume_port = 4000

    # Describes how to convert between HBase tables and DataFrames
    # This catalog makes use of composite keys
    # Types: string, int, float, double, boolean, tinyint (byte), smallint (short), bigint (long)
    __catalog = ''.join("""{
            "table":{"namespace":"default", "name":"service_cases"},
            "rowkey":"key_neighborhood:key_category:key_opened:key_case_id",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "category_id":{"cf":"rowkey", "col":"key_category", "type":"int"},
                "opened":{"cf":"rowkey", "col":"key_opened", "type":"int"},
                "case_id":{"cf":"rowkey", "col":"key_case_id", "type":"int"},

                "opened":{"cf":"a", "col":"opened", "type":"int"},
                "status_notes":{"cf":"a", "col":"status_notes", "type":"string"},
                "category":{"cf":"a", "col":"category", "type":"string"},
                "request_type":{"cf":"a", "col":"request_type", "type":"string"},
                "request_details":{"cf":"a", "col":"request_details", "type":"string"},
                "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},

                "address":{"cf":"l", "col":"address", "type":"string"},            
                "street":{"cf":"l", "col":"street", "type":"string"},            
                "latitude":{"cf":"l", "col":"latitude", "type":"double"},            
                "longitude":{"cf":"l", "col":"longitude", "type":"double"},

                "updated":{"cf":"m", "col":"updated", "type":"int"},
                "status":{"cf":"m", "col":"status", "type":"string"},
                "responsible_agency":{"cf":"m", "col":"responsible_agency", "type":"string"},
                "supervisor_district":{"cf":"m", "col":"supervisor_district", "type":"int"},
                "police_district":{"cf":"m", "col":"police_district", "type":"string"},
                "source":{"cf":"m", "col":"source", "type":"string"}
            }
        }""".split())

    def load_csv(self, spark: SparkSession) -> DataFrame:
        # Read csv file
        # The multiline config is necessary to support strings with line breaks in the csv file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option('quote', '"') \
            .option('escape', '"') \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.__file).limit(10000)

        # Remove rows missing category or location information
        # Some neighborhood names were simply numbers...
        df = df.where(df["Category"].isNotNull() & df["Neighborhood"].isNotNull() & df["Latitude"].isNotNull() &
                      df["Longitude"].isNotNull())

        # Select useful columns and rename them
        # We also convert timestamp strings into seconds since epoch and prepare some columns for our row keys
        df = df.select(unix_timestamp(to_timestamp("Opened", "MM/dd/yyyy hh:mm:ss a")).alias("opened"),
                       df["Status Notes"].alias("status_notes"),
                       df["Category"].alias("category"),
                       hasher("Category").alias("category_id"),
                       df["Request Type"].alias("request_type"),
                       df["Request Details"].alias("request_details"),
                       df["Neighborhood"].alias("neighborhood"),
                       hasher("Neighborhood").alias("neighborhood_id"),
                       df["Address"].alias("address"),
                       df["Street"].alias("street"),
                       df["Latitude"].cast(DoubleType()).alias("latitude"),
                       df["Longitude"].cast(DoubleType()).alias("longitude"),
                       df["Responsible Agency"].alias("responsible_agency"),
                       df["Supervisor District"].cast(IntegerType()).alias("supervisor_district"),
                       df["Police District"].alias("police_district"),
                       df["Source"].alias("source"),
                       unix_timestamp(to_timestamp("Updated", "MM/dd/yyyy hh:mm:ss a")).alias("updated"),
                       df["Status"].alias("status"),
                       df["CaseID"].cast(IntegerType()).alias("case_id"))

        # This would be optimal to include, but we do not have the processing power it requires!
        # Therefore, we keep the neighborhood information that comes with service cases, to allow us to ingest more data
        # neighborhood_boundaries_df = neighborhood_boundaries(spark)
        # df = df.join(
        #     neighborhood_boundaries_df,
        #     is_neighborhood_in_polygon("latitude", "longitude", "polygon"),
        #     "cross"
        # )
        #
        # df = df.drop("polygon")
        # df = df.withColumn("neighborhood_id", hasher(df["neighborhood"]))

        return df

    def load_flume(self, ssc: StreamingContext) -> DStream:
        stream = FlumeUtils.createStream(ssc, self.__flume_host, self.__flume_port)
        # Map applies an operation to each element in the stream, whereas transform applies an operation on an RDD level
        return stream.map(self.__parse_json) \
            .transform(lambda rdd: self.__convert_service_format(rdd))

    @staticmethod
    def __parse_json(data: str) -> Row:
        data_dict = json.loads(data[1])
        # Inferring schema from dict is deprecated, and it might even leave us with missing columns
        # Therefore, we create Row objects manually
        return Row(openedStr=data_dict.get("requested_datetime", ""),
                   status_notes=data_dict.get("status_notes", ""),
                   category=data_dict.get("service_name", ""),
                   request_type=data_dict.get("service_subtype", ""),
                   request_details=data_dict.get("service_details", ""),
                   address=data_dict.get("address", ""),
                   street=data_dict.get("street", ""),
                   latitude=float(data_dict.get("lat", "0")),
                   longitude=float(data_dict.get("long", "0")),
                   responsible_agency=data_dict.get("agency_responsible", ""),
                   supervisor_district=int(data_dict.get("supervisor_district", "-1")),
                   police_district=data_dict.get("police_district", ""),
                   source=data_dict.get("source", ""),
                   updatedStr=data_dict.get("updated_datetime", ""),
                   status=data_dict.get("status_description", ""),
                   case_id=data_dict.get("service_request_id", ""))

    @staticmethod
    def __convert_service_format(rdd: RDD) -> RDD:
        if rdd.isEmpty():
            return rdd
        df = rdd.toDF()

        # Since this method is invoked from a nested context on a driver, we must access the global SparkSession
        spark = get_spark_session_instance(rdd.context.getConf())
        neighborhood_boundaries_df = neighborhood_boundaries(spark)

        # Find neighborhoods from lat/lon
        # This is necessary, because a lot of the data from the API is missing neighborhood data
        df = df.join(
            neighborhood_boundaries_df,
            is_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        # Clean up after join
        df = df.drop("polygon")

        # Add key data and parse dates
        df = df.withColumn("category_id", hasher("category")) \
            .withColumn("neighborhood_id", hasher("neighborhood")) \
            .withColumn("opened", unix_timestamp(to_timestamp("openedStr", "yyyy-MM-dd'T'HH:mm:ss.SSS")).cast(IntegerType())) \
            .withColumn("updated", unix_timestamp(to_timestamp("updatedStr", "yyyy-MM-dd'T'HH:mm:ss.SSS")).cast(IntegerType())) \
            .drop("openedStr", "updatedStr")

        return df.rdd

    def load_hbase(self, spark: SparkSession) -> DataFrame:
        return spark.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
