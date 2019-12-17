from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, sum

from context.service_daily_aggregation_context import ServiceDailyAggregationContext
from context.service_monthly_aggregation_context import ServiceMonthlyAggregationContext
from context.service_running_aggregation_context import ServiceRunningAggregationContext

date_format = "yyyy-MM-dd"

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    context = ServiceRunningAggregationContext()
    df = context.load_hbase(spark)

    df.show(10, False)

    # Perform daily aggregates
    daily_df = df.withColumn("day", to_date(from_unixtime("time"), date_format)) \
        .groupBy("neighborhood_id", "category_id", "day") \
        .agg(sum("count").alias("count"))

    daily_to_save = daily_df.drop("day")
    daily_context = ServiceDailyAggregationContext()
    daily_context.save_hbase(daily_to_save)

    # Perform aggregation over the last month of data
    lastMonth = (datetime.now() - timedelta(days=30)).strftime(date_format)
    monthly_df = daily_df.orderBy("day") \
        .where(daily_df["day"] > to_date(lastMonth, date_format)) \
        .groupBy("neighborhood_id", "category_id") \
        .agg(sum("count").alias("count"))

    monthly_context = ServiceMonthlyAggregationContext()
    monthly_context.save_hbase(monthly_df)
