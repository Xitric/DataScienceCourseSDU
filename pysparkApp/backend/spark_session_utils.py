# From: http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session_instance(spark_conf: SparkConf):
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=spark_conf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]
