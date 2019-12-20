from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, count
from pyspark.sql.types import IntegerType

from context.aggregation_context import AggregationContext
from context.context import Context

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


def import_data(context: Context, spark: SparkSession, aggregator: AggregationContext, limit: int = 200000):
    # Store in HBase for further batch processing
    csv = load_newest(context, spark).limit(limit)
    context.save_hbase(csv)

    # Batch process 15 minute intervals
    aggregated = get_batch_processed(csv)
    aggregator.save_hbase(aggregated)
