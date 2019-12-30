# From: http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session_instance(spark_conf: SparkConf = None):
    if "sparkSessionSingletonInstance" not in globals():
        builder = SparkSession.builder
        if spark_conf:
            builder = builder.config(conf=spark_conf)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        spark.sparkContext.getConf().set("spark.executor.heartbeatInterval", "2000000")
        globals()["sparkSessionSingletonInstance"] = spark
    return globals()["sparkSessionSingletonInstance"]
