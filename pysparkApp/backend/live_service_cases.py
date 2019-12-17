import os
import sys

from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext

from service_aggregation_context import ServiceAggregationContext
from service_case_context import ServiceCaseContext
from string_hasher import string_hash

if os.path.exists('jobs.zip'):
    print("Success! YES!!")
    sys.path.insert(0, 'jobs.zip')
else:
    print("Well, shit...")


def save_to_hbase(rdd: RDD, ctx: ServiceCaseContext):
    if not rdd.isEmpty():
        ctx.save_hbase(rdd.toDF())


def save_aggregation(rdd: RDD, ctx: ServiceAggregationContext):
    if not rdd.isEmpty():
        df = rdd.toDF()
        df = df.select(df["_1"].alias("neighborhood"),
                       df["_2"].alias("category"),
                       unix_timestamp(df["_3"]).cast(IntegerType()).alias("time"),
                       df["_4"].cast(IntegerType()).alias("count"))

        # We need top define the UDF in here, since this function is executed on the workers whereas much of the code
        # outside of this function is executed on the driver
        hasher = udf(
            lambda value: string_hash(value),
            IntegerType()
        )

        df = df.withColumn("neighborhood_id", hasher("neighborhood")) \
            .withColumn("category_id", hasher("category"))
        df.show(10, False)

        ctx.save_hbase(df)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    ssc = StreamingContext(spark.sparkContext, 10)  # Check for new data every 10 seconds
    ssc.checkpoint("_checkpoint")

    service_context = ServiceCaseContext()
    dStream = service_context.load_flume(ssc)

    # Save raw data to HBase for later batch analysis
    dStream.foreachRDD(lambda rdd: save_to_hbase(rdd, service_context))
    dStream.pprint()

    # Convert to format for counting service cases
    neighborhood_category_stream = dStream.map(lambda row: (row.neighborhood, row.category))

    # Count service cases
    # Since time is seconds, we calculate aggregates over a 15 minute duration every 15 minutes
    # This pre-processing lowers the strain on the batch queries
    five_minute_aggregate_stream = neighborhood_category_stream \
        .map(lambda row: (row, 1)) \
        .reduceByKeyAndWindow(lambda agg, new: agg + new,
                              lambda agg, old: agg - old,
                              900, 900) \
        .transform(lambda time, rdd:
                   rdd.map(lambda row: (row[0][0], row[0][1], time, row[1])))

    aggregation_context = ServiceAggregationContext()
    five_minute_aggregate_stream.foreachRDD(lambda rdd: save_aggregation(rdd, aggregation_context))

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000000)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
