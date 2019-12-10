from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils
from pyspark.sql import SQLContext
import json

def handleRDD(rdd):
    if not rdd.isEmpty():
        rdd.toDF().show(10, True)

if __name__ == "__main__":
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    conf = SparkConf() \
        .set('spark.driver.host', '127.0.0.1') \
        .set("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.4")
    sc = SparkContext("local[2]", "SFGovIngestion", conf=conf)
    ssc = StreamingContext(sc, 1)
    sql = SQLContext(sc)
    print(sc.version)

    # Configure input DStream for Flume
    flumeStream = FlumeUtils.createStream(ssc, "pyspark", 4444)
    # flumeStream.map(lambda rdd: json.loads(rdd[1])).pprint()
    flumeStream.map(lambda rdd: json.loads(rdd[1])).foreachRDD(handleRDD)

    # Run for 2 minutes
    ssc.start()
    ssc.awaitTerminationOrTimeout(120)
    ssc.stop()
    # TODO: Json deserializer? The serialize data to json on the source?