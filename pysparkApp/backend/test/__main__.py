from pyspark.sql.session import SparkSession


def run():
    spark = SparkSession.builder \
        .appName("SfDataImporter") \
        .getOrCreate()
    print(spark.sparkContext.parallelize([1, 2, 3]).count())
    spark.stop()
