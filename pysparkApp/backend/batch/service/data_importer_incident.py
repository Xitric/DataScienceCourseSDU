from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, count
from pyspark.sql.types import IntegerType

from context.incident.incident_modern_context import IncidentModernContext
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext

truncate_time = udf(
    lambda precise_time: int(precise_time / 900) * 900,
    IntegerType()
)

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    context = IncidentModernContext()
    df = context.load_hbase(spark)

    # Batch process 15 minute intervals to optimize further jobs
    df_aggregated = df \
        .withColumn("time", truncate_time("incident_datetime")) \
        .groupBy("neighborhood_id", "incident_category_id", "neighborhood", "incident_category", "time") \
        .agg(count("*").alias("count"))

    df_aggregated = df_aggregated.select("neighborhood_id",
                                         "incident_category_id",
                                         "time",
                                         "neighborhood",
                                         "incident_category",
                                         df_aggregated["count"].cast(IntegerType()).alias("count"))

    aggregation_context = IncidentRunningAggregationContext()
    # aggregation_context = ServiceRunningAggregationContext()
    aggregation_context.save_hbase(df_aggregated)
