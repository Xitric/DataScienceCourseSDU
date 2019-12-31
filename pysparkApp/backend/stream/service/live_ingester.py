from datetime import datetime

from pyspark import RDD
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext

from context.aggregation_context import AggregationContext
from context.context import Context
from util.flume_ingestion_times import update_ingestion_times
from util.spark_session_utils import get_spark_session_instance
from util.string_hasher import string_hash


def __save_to_hbase(rdd: RDD, ctx: Context, data_source: str):
    if not rdd.isEmpty():
        df = rdd.toDF()
        ctx.save_hbase(df)

        df = df.orderBy(df["opened"].desc())
        latest = datetime.fromtimestamp(df.first()["opened"])
        update_ingestion_times(data_source, latest)


def __save_aggregation(rdd: RDD, ctx: AggregationContext):
    if not rdd.isEmpty():
        df = rdd.toDF()
        df = df.select(df["_1"].alias("neighborhood"),
                       df["_2"].alias("category"),
                       unix_timestamp(df["_3"]).cast(IntegerType()).alias("time"),
                       df["_4"].cast(IntegerType()).alias("count"))

        # We need to define the UDF in here, since this function is executed on the workers whereas much of the code
        # outside of this function is executed on the driver
        hasher = udf(
            lambda value: string_hash(value),
            IntegerType()
        )

        df = df.withColumn("neighborhood_id", hasher("neighborhood")) \
            .withColumn("category_id", hasher("category"))

        ctx.save_hbase(df)


def ingest(data_type: str, data_source: str, ctx: Context, agg_ctx: AggregationContext):
    spark = get_spark_session_instance()
    ssc = StreamingContext(spark.sparkContext, 10)  # Check for new data every 10 seconds
    ssc.checkpoint("_checkpoint_" + data_type)

    d_stream = ctx.load_flume(ssc)
    d_stream.pprint()

    # Save raw data to HBase for later batch analysis
    d_stream.foreachRDD(lambda rdd: __save_to_hbase(rdd, ctx, data_source))

    # Convert to format for counting service
    neighborhood_category_stream = d_stream.map(lambda row: (row.neighborhood, row.category))

    # TODO: Change window parameters to 15 minutes (900, 900)
    # Count records
    # Since time is seconds, we calculate aggregates over a 15 minute duration every 15 minutes
    # This pre-processing lowers the strain on the batch queries
    fifteen_minute_aggregate_stream = neighborhood_category_stream \
        .map(lambda row: (row, 1)) \
        .reduceByKeyAndWindow(lambda agg, new: agg + new,
                              lambda agg, old: agg - old,
                              60, 60) \
        .transform(lambda time, rdd:
                   rdd.map(lambda row: (row[0][0], row[0][1], time, row[1])))

    fifteen_minute_aggregate_stream.foreachRDD(lambda rdd: __save_aggregation(rdd, agg_ctx))

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000000)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
