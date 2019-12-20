from pyspark.sql.functions import to_date, from_unixtime
from pyspark.sql.types import FloatType

from batch.historical_import.data_importer import import_data
from context.incident.incident_historical_context import IncidentHistoricalContext
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext
from util.community_indicators import community_indicators
from util.spark_session_utils import get_spark_session_instance

java_date_format = "yyyy-MM-dd"

# Load historical data about old incident reports from csv files in HDFS
if __name__ == "__main__":
    spark = get_spark_session_instance()
    aggregation_context = IncidentRunningAggregationContext()

    import_data(IncidentHistoricalContext(),
                spark,
                aggregation_context)

    # Since historical incident reports are never updated, we process their daily counts in batch after import
    running_aggregated = aggregation_context.load_hbase(spark)

    # Prepare to normalize by population
    population_density = community_indicators(spark) \
        .select("neighborhood", "population_day")
    running_aggregated = running_aggregated.join(population_density, "neighborhood")

    # Perform daily aggregates
    daily_df = running_aggregated.withColumn("day", to_date(from_unixtime("time"), java_date_format)) \
        .groupBy("neighborhood_id", "category_id", "neighborhood", "category", "day", "population_day") \
        .agg(sum("count").alias("count"))

    # Convert counts to rates normalized by population
    daily_to_save = daily_df.withColumn("rate", (daily_df["count"] / daily_df["population_day"]).cast(FloatType())) \
        .drop("count") \
        .drop("population_day") \
        .drop("neighborhood_id") \
        .drop("category_id")

    # Save to MySQL
    daily_to_save.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='incident_cases_daily',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('append').save()
