from pyspark.sql import SparkSession

from service_aggregation_context import ServiceAggregationContext

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    context = ServiceAggregationContext()
    df = context.load_hbase(spark)

    df.show(10, False)
