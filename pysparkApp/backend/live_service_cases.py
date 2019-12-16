from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext

from service_aggregation_context import ServiceAggregationContext
from service_case_context import ServiceCaseContext
from string_hasher import string_hash


def save_to_hbase(rdd: RDD, ctx: ServiceCaseContext):
    if not rdd.isEmpty():
        ctx.save_hbase(rdd.toDF())


def save_aggregation(rdd: RDD, ctx: ServiceAggregationContext):
    if not rdd.isEmpty():
        df = rdd.toDF()
        df = df.select(df["_1"].alias("neighborhood"),
                       df["_2"].alias("category"),
                       unix_timestamp(df["_3"]).alias("time"),
                       df["_4"].alias("count"))

        # We need top define the UDF in here, since this function is executed on the workers whereas much of the code
        # outside of this function is executed on the driver
        hasher = udf(
            lambda value: string_hash(value),
            IntegerType()
        )

        df = df.withColumn("neighborhood_id", hasher("neighborhood")) \
            .withColumn("category_id", hasher("category"))

        ctx.save_hbase(df)


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    ssc = StreamingContext(spark.sparkContext, 1)
    ssc.checkpoint("_checkpoint")

    service_context = ServiceCaseContext()
    dStream = service_context.load_flume(ssc)

    # Save raw data to HBase for batch analysis
    dStream.foreachRDD(lambda rdd: save_to_hbase(rdd, service_context))

    # Convert to format for counting service cases
    neighborhood_category_stream = dStream.map(lambda row: (row.neighborhood, row.category))

    # TODO: Fix window and sliding values when Flume sends data continuously rather than daily
    # Count service cases
    # Imagine time is minutes, we calculate aggregates over a 5 minute duration every five minutes
    five_minute_aggregate_stream = neighborhood_category_stream \
        .map(lambda row: (row, 1)) \
        .reduceByKeyAndWindow(lambda agg, new: agg + new,
                              lambda agg, old: agg - old,
                              1, 1) \
        .transform(lambda time, rdd:
                   rdd.map(lambda row: (row[0][0], row[0][1], time, row[1])))

    aggregation_context = ServiceAggregationContext()
    five_minute_aggregate_stream.foreachRDD(lambda rdd: save_aggregation(rdd, aggregation_context))

    ssc.start()
    ssc.awaitTerminationOrTimeout(80)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)

    # Previous attempts
    # neighborhood_category_stream.map(lambda row: (row, 1)) \
    #     .updateStateByKey(lambda new, running: sum(new, running or 0)) \
    #     .window(5, 1) \
    #     .map(lambda row: (row[0], row[1] / 10)) \
    #     .pprint()

    # neighborhood_category_stream.countByValueAndWindow(5, 1) \
    #     .updateStateByKey(lambda new, running: sum(new)) \
    #     .map(lambda row: (row[0], row[1] / 10)) \
    #     .pprint()

    # neighborhood_category_stream.countByValueAndWindow(5, 1) \
    #     .map(lambda row: (row[0], row[1] / 10)) \
    #     .pprint()

    # neighborhood_category_stream \
    #     .transform(lambda time, rdd:
    #                rdd.map(lambda row: ((row[0], row[1], time), 1))) \
    #     .reduceByKey(lambda running, new: running + new) \
    #     .pprint()

    # Test data
    # neighborhood_category_stream = ssc.queueStream([
    #     spark.sparkContext.parallelize([("NB1", "Service 1"), ("NB1", "Service 2"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB1", "Service 1"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB3", "Service 1"), ("NB2", "Service 2"), ("NB3", "Service 2")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB3", "Service 3"), ("NB3", "Service 3")]),
    #     spark.sparkContext.parallelize([("NB1", "Service 3"), ("NB1", "Service 1"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB1", "Service 1"), ("NB1", "Service 2"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB1", "Service 1"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB3", "Service 1"), ("NB2", "Service 2"), ("NB3", "Service 2")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB3", "Service 3"), ("NB3", "Service 3")]),
    #     spark.sparkContext.parallelize([("NB1", "Service 3"), ("NB1", "Service 1"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB1", "Service 1"), ("NB1", "Service 2"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB1", "Service 1"), ("NB2", "Service 1")]),
    #     spark.sparkContext.parallelize([("NB3", "Service 1"), ("NB2", "Service 2"), ("NB3", "Service 2")]),
    #     spark.sparkContext.parallelize([("NB2", "Service 2"), ("NB3", "Service 3"), ("NB3", "Service 3")]),
    #     spark.sparkContext.parallelize([("NB1", "Service 3"), ("NB1", "Service 1"), ("NB2", "Service 1")])
    # ], oneAtATime=True,
    #     default=spark.sparkContext.parallelize([("NB1", "Service 3"), ("NB1", "Service 1"), ("NB2", "Service 1")]))
