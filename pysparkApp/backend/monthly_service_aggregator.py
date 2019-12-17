from pyspark.sql import SparkSession

from service_aggregation_context import ServiceAggregationContext

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    context = ServiceAggregationContext()
    df = context.load_hbase(spark)

    df.show(10, False)

    # Query the past 4 weeks
    # since = datetime.now() - timedelta(weeks=4)
    # since_seconds = int(since.timestamp())
    # month_df = df.where(df["time"] > since_seconds)
    #
    # # TODO: Does it result in a better plan if we use neighborhood_id and category_id?
    # # Aggregate batches for the entire month
    # aggregated_month_df = month_df.groupBy("neighborhood", "category") \
    #     .agg(sum("count"))
    #
    # # TODO: Save to HBase
    #
    # aggregated_month_df.show(10, False)
    #
    # # Compute daily aggregates
    # aggregated_day = df.withColumn("day", to_date("time", "dd-MM-yyyy")) \
    #     .groupBy("neighborhood", "category", "day") \
    #     .agg(sum("count")) \
    #
    # # TODO: Save to HBase
    #
    # aggregated_day.show(10, False)
