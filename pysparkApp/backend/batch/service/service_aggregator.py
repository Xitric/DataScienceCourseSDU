from datetime import datetime, timedelta

from geo_pyspark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, sum, lit, first
from pyspark.sql.types import FloatType

from context.service.service_running_aggregation_context import ServiceRunningAggregationContext
from util.community_indicators import community_indicators

java_date_format = "yyyy-MM-dd"
python_date_format = "%Y-%m-%d"

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
    daily_df = df.withColumn("day", to_date(from_unixtime("time"), java_date_format)) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "day", "population_day") \
        .agg(sum("count").alias("count"))

    # Convert counts to rates normalized by population
    # [neighborhood, category, rate, day]
    daily_to_save = daily_df.withColumn("rate", (daily_df["count"] / daily_df["population_day"]).cast(FloatType())) \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("category_id")

    # Save to MySQL
    daily_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_daily',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('append').save()

    # Perform aggregation over the last month of data
    # [neighborhood_id, category_id, neighborhood, category, count, month, population_day]
    thisMonth = datetime.now().strftime(python_date_format)
    lastMonth = (datetime.now() - timedelta(days=30)).strftime(python_date_format)
    monthly_df = daily_df.orderBy("day") \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "population_day") \
        .agg(sum("count").alias("count")) \
        .withColumn("month", to_date(lit(thisMonth), java_date_format))

    # Convert counts to rates normalized by population
    # [neighborhood, category, rate, month]
    monthly_to_save = monthly_df.withColumn("rate", (monthly_df["count"] / monthly_df["population_day"])
                                            .cast(FloatType())) \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("category_id")

    # Convert many rows into one large row for each neighborhood that specifies rates for every category that month
    # monthly_to_save = monthly_to_save.where(monthly_to_save["category"]
    #                                         .isin(lit("Encampments"), lit("Illegal Postings"), lit("Street Defects")))
    # monthly_to_save.explain()
    # monthly_to_save = monthly_to_save.groupBy("neighborhood", "month") \
    #     .pivot("category") \
    #     .agg(first("rate").alias("rate"))
    monthly_to_save = monthly_to_save.groupBy("neighborhood", "month") \
        .pivot("category") \
        .agg(first("rate").alias("rate"))

    # Save to MySQL
    monthly_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()
