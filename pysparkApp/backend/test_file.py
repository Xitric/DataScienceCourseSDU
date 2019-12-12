from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .config("spark.jars", "/backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar") \
    .config("spark.driver.host", "172.200.0.55") \
    .config("spark.driver.port", "43345") \
    .appName("SfDataImporter") \
    .getOrCreate()
print(spark.sparkContext.parallelize([1,2,3]).count())
