from datetime import datetime, timedelta

from geo_pyspark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, sum, unix_timestamp, lit
from pyspark.sql.types import FloatType, IntegerType

from context.service_daily_aggregation_context import ServiceDailyAggregationContext
from context.service_monthly_aggregation_context import ServiceMonthlyAggregationContext
from context.service_running_aggregation_context import ServiceRunningAggregationContext
from util.community_indicators import community_indicators

date_format = "yyyy-MM-dd"

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    GeoSparkRegistrator.registerAll(spark)

    context = ServiceRunningAggregationContext()
    df = context.load_hbase(spark)

    # TODO: Use hdfs to store the last time from which data has been aggregated, so we do not need to aggregate
    #  everything anew. Then the job can be run every day to support streaming

    # Prepare to normalize by population
    # [neighborhood, population_day]
    population_density = community_indicators(spark) \
        .select("neighborhood", "population_day")
    df = df.join(population_density, "neighborhood")

    # Perform daily aggregates
    # [neighborhood_id, category_id, neighborhood, category, count, day, population_day]
    daily_df = df.withColumn("day", to_date(from_unixtime("time"), date_format)) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "day", "population_day") \
        .agg(sum("count").alias("count"))

    # Convert counts to rates normalized by population
    # [neighborhood_id, category_id, neighborhood, category, rate, time]
    daily_to_save = daily_df.withColumn("rate", (daily_df["count"] / daily_df["population_day"]).cast(FloatType())) \
        .drop("count") \
        .drop("population_day") \
        .withColumn("time", unix_timestamp("day", date_format).cast(IntegerType())) \
        .drop("day")
    daily_context = ServiceDailyAggregationContext()
    daily_context.save_hbase(daily_to_save)

    # Perform aggregation over the last month of data
    # [neighborhood_id, category_id, neighborhood, category, count, time, population_day]
    thisMonth = datetime.now().strftime(date_format)
    lastMonth = (datetime.now() - timedelta(days=30)).strftime(date_format)
    monthly_df = daily_df.orderBy("day") \
        .where(daily_df["day"] > to_date(lit(lastMonth), date_format)) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "population_day") \
        .agg(sum("count").alias("count")) \
        .withColumn("time", unix_timestamp(lit(thisMonth), date_format).cast(IntegerType()))

    # Convert counts to rates normalized by population
    # [neighborhood_id, category_id, neighborhood, category, rate, time]
    monthly_to_save = monthly_df.withColumn("rate", (monthly_df["count"] / monthly_df["population_day"])
                                            .cast(FloatType())) \
        .drop("count") \
        .drop("population_day")

    monthly_context = ServiceMonthlyAggregationContext()
    monthly_context.save_hbase(monthly_df)

    daily_context.load_hbase(spark).show(50, False)
    monthly_context.load_hbase(spark).show(50, False)
