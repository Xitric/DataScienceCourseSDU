from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .appName("SfDataImporter") \
    .master("yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.driver.host", "172.200.0.55") \
    .config("spark.driver.port", "43345") \
    .getOrCreate()
print(spark.sparkContext.parallelize([1,2,3]).count())
