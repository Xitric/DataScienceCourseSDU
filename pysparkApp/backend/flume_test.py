from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from service_case_context import ServiceCaseContext


def handle_service_case(rdd: RDD, ctx: ServiceCaseContext):
    if not rdd.isEmpty():
        ctx.save_hbase(rdd.toDF())


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("SFGovIngestion") \
        .config('spark.driver.host', '127.0.0.1') \
        .config("spark.jars", "/backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar") \
        .config("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.4") \
        .getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 1)

    context = ServiceCaseContext()
    context.load_flume(ssc).foreachRDD(lambda rdd: handle_service_case(rdd, context))

    ssc.start()
    ssc.awaitTerminationOrTimeout(50)
    ssc.stop()
