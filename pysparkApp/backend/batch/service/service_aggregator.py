import os
from datetime import datetime, timedelta

from geo_pyspark.register import GeoSparkRegistrator
from py4j.protocol import Py4JJavaError
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
    spark.sparkContext.getConf().set("spark.executor.heartbeatInterval", "2000000")
    GeoSparkRegistrator.registerAll(spark)

    timestamp_file_path = os.environ["CORE_CONF_fs_defaultFS"] + "/aggregator_timestamps/service.csv"
    service_timestamp = 0


    def path_exist(path):
        try:
            rdd = spark.sparkContext.textFile(path)
            rdd.take(1)
            return True
        except Py4JJavaError as _:
            return False


    if path_exist(timestamp_file_path):
        service_timestamp = spark.read.format("csv").load(timestamp_file_path).collect()[0][0]  # first value in the row
        print("file exists with timestamp: " + str(service_timestamp))

    context = ServiceRunningAggregationContext()
    df = context.load_hbase_timestamp(spark, int(service_timestamp))
    df.show(10, True)
    print(df.count())

    # check file in hdfs, if a timetamp exists, take the timestamp-1 day and compute from there

    # problem: beregner alt data fra de sidste mange år og gemmer det i mysql, når der kommer ny data er der
    # ingen grund til at bergene alt data + det nye og så gemme det i mysql. Da vi har beregnet det gamle og gemt i mysql.
    # Så gem hvor vi er nået til og bergen data for nuværende dag + den forrige og gem dette
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
    daily_to_save = daily_df.orderBy("day", ascending=False) \
        .withColumn("rate", (daily_df["count"] / daily_df["population_day"]).cast(FloatType())) \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("category_id")

    latest_date_from_df = daily_to_save.select(daily_to_save["day"]).collect()[0][0]
    latest_date = datetime.strptime(str(latest_date_from_df), "%Y-%m-%d")
    date_in_seconds = int((latest_date - datetime.utcfromtimestamp(0)).total_seconds())
    print(date_in_seconds)

    # Save to MySQL
    daily_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_daily',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('append').save()

    daily_to_save.show(10, True)

    timestamp_df = spark.createDataFrame([(date_in_seconds,)], ["date"])  # spark expects a tuple
    timestamp_df.repartition(1).write.format("csv").mode("overwrite").save("/aggregator_timestamps/service.csv")

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
