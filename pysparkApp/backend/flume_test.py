from pyspark import SparkContext, SparkConf, RDD
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SQLContext
import json


def get_service_dstream() -> DStream:
    return FlumeUtils.createPollingStream(ssc, [("flume", 4000)])


def get_incident_dstream() -> DStream:
    return FlumeUtils.createPollingStream(ssc, [("flume", 4001)])


def handle_service_case(rdd: RDD):
    if not rdd.isEmpty():
        rdd.toDF().show(10, False)


def handle_incident_report(rdd: RDD):
    if not rdd.isEmpty():
        rdd.toDF().show(10, False)


if __name__ == "__main__":
    # TODO: Can we use a spark session instead? That way we are not using different techniques in different places
    # Create a local StreamingContext with two working threads and batch interval of 1 second
    conf = SparkConf() \
        .set('spark.driver.host', '127.0.0.1') \
        .set("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.4")
    sc = SparkContext("local[2]", "SFGovIngestion", conf=conf)
    ssc = StreamingContext(sc, 1)
    sql = SQLContext(sc)
    print(sc.version)

    # Configure input DStreams for Flume
    get_service_dstream().map(lambda rdd: json.loads(rdd[1])).foreachRDD(handle_service_case)
    get_incident_dstream().map(lambda rdd: json.loads(rdd[1])).foreachRDD(handle_incident_report)

    # Process data
    # flumeStream.map(lambda rdd: json.loads(rdd[1])).foreachRDD(handleRDD)
    # serviceStream.map(lambda rdd: json.loads(rdd[1])).foreachRDD(handle_service_case)
    # policeStream.map(lambda rdd: json.loads(rdd[1])).foreachRDD(handle_incident_report)

    # Run for 2 minutes
    ssc.start()
    ssc.awaitTerminationOrTimeout(120)
    ssc.stop()
