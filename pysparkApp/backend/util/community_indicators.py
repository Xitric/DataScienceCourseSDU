import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import substring_index, udf
from pyspark.sql.types import FloatType

from util.neighborhood_boundaries import neighborhood_boundaries

calculate_area = udf(
    lambda polygon: polygon.area,
    FloatType()
)


def community_indicators(spark: SparkSession) -> DataFrame:
    indicators_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/Community_Resiliency_Indicator_System.csv"
    indicators = spark.read \
        .format("csv") \
        .option("header", "true") \
        .load(indicators_file)

    # Read csv file
    indicators = indicators.select(indicators["Neighborhood"].alias("neighborhood"),
                                   indicators["Haz_Score"].alias("hazard_score"),
                                   indicators["Env_Score"].alias("environment_score"),
                                   indicators["VCrim_Rate"].alias("violent_crime_rate"),
                                   indicators["Citz_Per"].alias("citizen_density"),
                                   indicators["Com_Score"].alias("community_score"),
                                   indicators["Food_Score"].alias("food_score"),
                                   indicators["Emp_per"].alias("employment_rate"),
                                   indicators["PopDens"].alias("population_density"),
                                   indicators["DayPopDens"].alias("population_density_day"))

    # Separate neighborhoods that have been combined with a "/"
    paired_neighborhoods = indicators.where(indicators["neighborhood"].like("%/%"))
    left_of_pairs = paired_neighborhoods.withColumn("neighborhood",
                                                    substring_index(paired_neighborhoods["neighborhood"], "/", 1))
    right_of_pairs = paired_neighborhoods.withColumn("neighborhood",
                                                     substring_index(paired_neighborhoods["neighborhood"], "/", -1))
    indicators = indicators.where(~indicators["neighborhood"].like("%/%")) \
        .unionAll(left_of_pairs) \
        .unionAll(right_of_pairs)

    # Use neighborhood areas to convert population density to population count
    areas = neighborhood_boundaries(spark) \
        .withColumn("area", calculate_area("polygon")) \
        .drop("polygon")

    indicators = indicators.join(areas, "neighborhood")
    indicators = indicators.withColumn("population", indicators["population_density"] * indicators["area"]) \
        .withColumn("population_day", indicators["population_density_day"] * indicators["area"]) \
        .drop("area")

    return indicators
