import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, unix_timestamp
from pyspark.sql.types import IntegerType, DoubleType, LongType
from pyspark.streaming import StreamingContext, DStream

from context.context import Context
from util.neighborhood_boundaries import neighborhood_boundaries, is_neighborhood_in_polygon
from util.string_hasher import string_hash

string_to_hash = udf(
    lambda string: string_hash(string),
    IntegerType()
)


class IncidentHistoricalContext(Context):
    # File from HDFS
    incident_modern_file = os.environ["CORE_CONF_fs_defaultFS"] \
                           + "/datasets/Police_Department_Incident_Reports__Historical_2003_to_May_2018.csv"

    __catalog = ''.join("""{
            "table":{"namespace":"default", "name":"historical_incident_reports"},
            "rowkey":"key_neighborhood:key_category:key_date:key_pd_id",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "category_id":{"cf":"rowkey", "col":"key_category", "type":"int"},
                "date":{"cf":"rowkey", "col":"key_date", "type":"int"},
                "pd_id":{"cf":"rowkey", "col":"key_pd_id", "type":"int"},
                "neighborhood":{"cf":"a", "col":"neighborhood", "type":"string"},
                "category":{"cf":"a", "col":"category", "type":"string"},
                "date":{"cf":"a", "col":"date", "type":"int"},
                "time":{"cf":"a", "col":"time", "type":"int"},
                "resolution":{"cf":"a", "col":"resolution", "type":"string"},
                "incidnt_num":{"cf":"a", "col":"incidnt_num", "type":"string"},
                "descript":{"cf":"a", "col":"descript", "type":"string"},
                "address":{"cf":"l", "col":"address", "type":"string"},
                "latitude":{"cf":"l", "col":"latitude", "type":"double"},
                "longitude":{"cf":"l", "col":"longitude", "type":"double"},
                "pd_district":{"cf":"l", "col":"pd_district", "type":"string"}
              } 
    }""".split())

    def load_csv(self, spark: SparkSession) -> DataFrame:
        # Read csv file
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("multiline", "true") \
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
            .load(self.incident_modern_file)

        # Remove rows missing category or location information
        df = df.where(df["Category"].isNotNull() & df["X"].isNotNull() & df["Y"].isNotNull())

        df = df.select(
            df["PdId"].cast(LongType()).alias("pd_id"),
            df["Category"].alias("category"),
            string_to_hash("Category").alias("category_id"),
            unix_timestamp("Date", "MM/dd/yyyy").cast(IntegerType()).alias("date"),
            unix_timestamp("Time", "HH:mm").cast(IntegerType()).alias("time"),
            df["Resolution"].alias("resolution"),
            df["IncidntNum"].alias("incidnt_num"),
            df["Descript"].alias("descript"),
            df["Address"].alias("address"),
            df["X"].cast(DoubleType()).alias("longitude"),
            df["Y"].cast(DoubleType()).alias("latitude"),
            df["PdDistrict"].alias("pd_district")
        )

        neighborhood_boundaries_df = neighborhood_boundaries(spark)

        # Join df and neighborhood_boundaries_df if latitude and longitude is in the polygon
        df = df.join(
            neighborhood_boundaries_df,
            is_neighborhood_in_polygon("latitude", "longitude", "polygon"),
            "cross"
        )

        df = df.drop("polygon")
        df = df.withColumn("neighborhood_id", string_to_hash(df["neighborhood"]))

        return df

    def load_hbase(self, session: SparkSession) -> DataFrame:
        return session.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
