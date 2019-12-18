from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count
from pyspark.sql.types import IntegerType

from context.service_case_context import ServiceCaseContext
from context.service_running_aggregation_context import ServiceRunningAggregationContext

truncate_time = udf(
    lambda precise_time: int(precise_time / 900) * 900,
    IntegerType()
)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    service_context = ServiceCaseContext()
    service_csv = service_context.load_csv(spark)

    # Store in HBase for further batch processing
    service_context.save_hbase(service_csv)

    # TODO: for aggregating after the data is imported into HBase
    # service_csv = service_context.load_hbase(spark)

    # Batch process 15 minute intervals to optimize further jobs
    service_aggregated = service_csv.withColumn("time", truncate_time("opened")) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "time") \
        .agg(count("*").alias("count"))

    service_aggregated = service_aggregated.select("neighborhood_id",
                                                   "category_id",
                                                   "time",
                                                   "neighborhood",
                                                   "category",
                                                   service_aggregated["count"].cast(IntegerType()).alias("count"))

    aggregation_context = ServiceRunningAggregationContext()
    aggregation_context.save_hbase(service_aggregated)
