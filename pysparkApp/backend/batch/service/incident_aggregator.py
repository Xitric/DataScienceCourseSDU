from datetime import datetime, timedelta

from geo_pyspark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, to_date, sum, lit, first
from pyspark.sql.types import FloatType

from batch.service.service_aggregator import python_date_format, java_date_format
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext
from util.community_indicators import community_indicators

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    GeoSparkRegistrator.registerAll(spark)

    context = IncidentRunningAggregationContext()
    df = context.load_hbase(spark)

    population_density = community_indicators(spark) \
        .select("neighborhood", "population_day")
    df = df.join(population_density, "neighborhood")

    daily_df = df.withColumn("day", to_date(from_unixtime("time"), java_date_format)) \
        .groupBy("neighborhood_id", "incident_category_id", "neighborhood", "incident_category", "day",
                 "population_day") \
        .agg(sum("count").alias("count"))

    daily_to_save = daily_df.withColumn("rate", (daily_df["count"] / daily_df["population_day"]).cast(FloatType())) \
        .withColumnRenamed("incident_category", "category") \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("incident_category_id")

    # Save to MySQL
    daily_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='incident_cases_daily',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()

    thisMonth = datetime.now().strftime(python_date_format)
    lastMonth = (datetime.now() - timedelta(days=30)).strftime(python_date_format)
    monthly_df = daily_df.orderBy("day") \
        .groupBy("neighborhood_id", "incident_category_id", "neighborhood", "incident_category", "population_day") \
        .agg(sum("count").alias("count")) \
        .withColumn("month", to_date(lit(thisMonth), java_date_format))

    monthly_to_save = monthly_df.withColumn("rate", (monthly_df["count"] / monthly_df["population_day"])
                                            .cast(FloatType())) \
        .withColumnRenamed("incident_category", "category") \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("incident_category_id")

    # Convert many rows into one large row for each neighborhood that specifies rates for every category that month
    monthly_to_save = monthly_to_save \
        .groupBy("neighborhood", "month") \
        .pivot("category") \
        .agg(first("rate").alias("rate"))

    monthly_to_save.show(10, False)

    # Save to MySQL
    monthly_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='incident_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()
