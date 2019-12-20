from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, udf
from pyspark.sql.types import IntegerType
from pyspark.streaming import StreamingContext

from context.incident_aggregation_context import IncidentAggregationContext
from context.incident_modern_context import IncidentModernContext
from util.string_hasher import string_hash


def save_to_hbase(rdd: RDD, ctx: IncidentModernContext):
    if not rdd.isEmpty():
        ctx.save_hbase(rdd.toDF())


def save_aggregation(rdd: RDD, ctx: IncidentAggregationContext):
    if not rdd.isEmpty():
        df = rdd.toDF()

        df = df.select(df["_1"].alias("neighborhood"),
                       df["_2"].alias("category"),
                       unix_timestamp(df["_3"]).cast(IntegerType()).alias("time"),
                       df["_4"].cast(IntegerType()).alias("count"))

        df.show(10, True)
        # We need top define the UDF in here, since this function is executed on the workers whereas much of the code
        # outside of this function is executed on the driver
        hasher = udf(
            lambda value: string_hash(value),
            IntegerType()
        )

        df = df.withColumn("neighborhood_id", hasher("neighborhood")) \
            .withColumn("category_id", hasher("category"))

        ctx.save_hbase(df)


def save(rdd: RDD, ctx: IncidentModernContext):
    if not rdd.isEmpty():
        #rdd.toDF().show(10, True)
        save_to_hbase(rdd, ctx)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    ssc = StreamingContext(spark.sparkContext, 1)
    ssc.checkpoint("_checkpoint")

    incident_context = IncidentModernContext()
    d_stream = incident_context.load_flume(ssc)
    #d_stream.pprint()

    d_stream.foreachRDD(lambda rdd: save(rdd, incident_context))

    neighborhood_category_stream = d_stream.map(lambda row: (row.neighborhood, row.incident_category))

   # neighborhood_category_stream.pprint()

    fifteen_minute_aggregate_stream = neighborhood_category_stream \
        .countByValueAndWindow(1, 1) \
        .transform(lambda time, rdd: rdd.map(lambda row: (row[0][0], row[0][1], time, row[1])))

    #fifteen_minute_aggregate_stream.pprint()
    aggregation_context = IncidentAggregationContext()
    fifteen_minute_aggregate_stream.foreachRDD(lambda rdd: save_aggregation(rdd, aggregation_context))

    ssc.start()
    ssc.awaitTerminationOrTimeout(10000000)
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
