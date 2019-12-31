from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, count
from pyspark.sql.types import IntegerType

from context.aggregation_context import AggregationContext
from context.context import Context
from util.flume_ingestion_times import update_ingestion_times

truncate_time = udf(
    lambda precise_time: int(precise_time / 900) * 900,
    IntegerType()
)


def load_newest(context: Context, spark: SparkSession) -> DataFrame:
    df = context.load_csv(spark)
    return df.orderBy(df["opened"].desc())


# Batch process 15 minute intervals to optimize further jobs
def get_batch_processed(df: DataFrame) -> DataFrame:
    aggregated = df.withColumn("time", truncate_time("opened")) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "time") \
        .agg(count("*").alias("count"))

    return aggregated.select("neighborhood_id",
                             "category_id",
                             "time",
                             "neighborhood",
                             "category",
                             aggregated["count"].cast(IntegerType()).alias("count"))


def import_data(context: Context, spark: SparkSession, aggregator: AggregationContext, data_source: str,
                limit: int = 50000):
    # Store in HBase for further batch processing
    print("Start: " + str(datetime.now()))
    csv = load_newest(context, spark)
    context.save_hbase(csv)
    print("End: " + str(datetime.now()))

    # Update ingestion times for Flume
    latest = datetime.fromtimestamp(csv.first()["opened"])
    update_ingestion_times(data_source, latest)

    # Batch process 15 minute intervals
    aggregated = get_batch_processed(csv)
    aggregator.save_hbase(aggregated)
