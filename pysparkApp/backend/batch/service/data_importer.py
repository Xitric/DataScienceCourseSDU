from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count
from pyspark.sql.types import IntegerType

from context.service_case_context import ServiceCaseContext
from context.service_running_aggregation_context import ServiceRunningAggregationContext
from util.string_hasher import string_hash

truncate_time = udf(
    lambda precise_time: int(precise_time / 900) * 900,
    IntegerType()
)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # We limit the data import because of resource constraints
    service_context = ServiceCaseContext()
    service_csv = service_context.load_csv(spark) \
        .limit(50000)

    # Store in HBase for further batch processing
    service_context.save_hbase(service_csv)

    # Batch process 15 minute intervals to optimize further jobs
    service_aggregated = service_csv.withColumn("time", truncate_time("opened")) \
        .groupBy("neighborhood_id", "category_id", "time") \
        .agg(count("*").alias("count")) \
        .select("neighborhood_id", "category_id",
                "time", "neighborhood",
                "category", "count")

    aggregation_context = ServiceRunningAggregationContext()
    aggregation_context.save_hbase(service_aggregated)

    # Test
    service_hbase = aggregation_context.load_hbase(spark)
    service_hbase = service_hbase.where(service_hbase["neighborhood_id"] == string_hash("Financial District"))
    service_hbase.show(1000, False)
