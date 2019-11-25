import os

from context import Context
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, length, unix_timestamp, to_timestamp
from pyspark.sql.types import IntegerType, DoubleType


class ServiceCaseContext(Context):
    """A class for easily creating DataFrames of 311 service cases"""

    # The file to read from HDFS
    __file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/311_Cases.csv"

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

    def __init__(self, session: SparkSession):
        super().__init__(session)

    def load_csv(self) -> DataFrame:
        # Read csv file
        # The multiline config is necessary to support strings with line breaks in the csv file
        df = self.spark.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option('quote', '"') \
            .option('escape', '"') \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.__file)

        # Remove rows missing category or location information
        # Some neighborhood names were simply numbers...
        df = df.where(df["Category"].isNotNull() & df["Neighborhood"].isNotNull() &
                      (length(df["Neighborhood"]) > 1) & (length(df["Category"]) > 1))

        # User defined function for calculating category and neighborhood numbers
        category_id = udf(
            lambda category_str: hash(category_str),  # Conversion function
            IntegerType()  # Return type
        )
        neighborhood_id = udf(
            lambda neighborhood_str: hash(neighborhood_str),
            IntegerType()
        )

        # Select useful columns and rename them
        # We also convert timestamp strings into seconds since epoch and prepare some columns for our row keys
        df = df.select(unix_timestamp(to_timestamp("Opened", "MM/dd/yyyy hh:mm:ss a")).alias("opened"),
                       df["Status Notes"].alias("status_notes"),
                       df["Category"].alias("category"),
                       category_id("Category").alias("category_id"),
                       df["Request Type"].alias("request_type"),
                       df["Request Details"].alias("request_details"),
                       df["Neighborhood"].alias("neighborhood"),
                       neighborhood_id("Neighborhood").alias("neighborhood_id"),
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

        return df

    def load_hbase(self) -> DataFrame:
        return self.spark.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
