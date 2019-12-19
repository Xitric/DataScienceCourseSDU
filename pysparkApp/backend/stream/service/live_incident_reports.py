from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

from context.incident_modern_context import IncidentModernContext


def func(rdd: RDD):
    if rdd.isEmpty():
        return

    rdd.toDF().show(10)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    ssc = StreamingContext(spark.sparkContext, 2)
    ssc.checkpoint("_checkpoint")

    incident_context = IncidentModernContext()
    d_stream = incident_context.load_flume(ssc)
    d_stream.pprint()
   # d_stream.foreachRDD(func)

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000000)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
