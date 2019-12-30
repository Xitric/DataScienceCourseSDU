import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, ArrayType, StringType
from shapely.geometry import Polygon, Point

from util.spark_session_utils import get_spark_session_instance

__polygon = udf(
    lambda neighborhood_boundary_string: __create_polygon(neighborhood_boundary_string),
    ArrayType(ArrayType(FloatType()))
)


# Wrap udf in a function to allow us to pass array literals
def __neighborhood(polygons: [[[float, float]]], neighborhoods: [str]):
    return udf(
        lambda lat, lon: __get_neighborhood_from(lat, lon, polygons, neighborhoods),
        StringType()
    )


def __get_neighborhood_from(lat: float, lon: float, polygons: [[[float, float]]], neighborhoods: [str]) -> str:
    for (i, p) in enumerate(polygons):
        if Polygon(p).contains(Point(float(lat), float(lon))):
            return neighborhoods[i]
    return "Missing neighborhood"


# Create and return an array of points based on the neighborhood boundary string
def __create_polygon(neighborhood_boundary_string: str) -> [[float, float]]:
    neighborhood_boundary = neighborhood_boundary_string[16:-3]
    points_with_spaces = neighborhood_boundary.split(", ")
    points = []
    for point in points_with_spaces:
        coordinates = point.split(" ")
        points.append(Point(float(coordinates[1]), float(coordinates[0])))
    return [[p.x, p.y] for p in points]


# Read the neighborhood file from HDFS and create polygons based on the neighborhood boundaries
def neighborhood_boundaries(spark: SparkSession) -> DataFrame:
    sf_boundaries_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/SFFind_Neighborhoods.csv"
    sf_boundaries_df = spark.read.format("csv").option("header", "true").load(sf_boundaries_file)
    sf_boundaries_df = sf_boundaries_df.withColumn("the_geom", __polygon(sf_boundaries_df["the_geom"]))
    sf_boundaries_df = sf_boundaries_df.select(
        sf_boundaries_df["the_geom"].alias("polygon"),
        sf_boundaries_df["name"].alias("neighborhood")
    )
    sf_boundaries_df.cache()
    return sf_boundaries_df


def add_neighborhoods(df: DataFrame) -> DataFrame:
    file_contents = neighborhood_boundaries(get_spark_session_instance()).collect()
    boundaries = [entry[0] for entry in file_contents]
    neighborhoods = [entry[1] for entry in file_contents]

    return df.withColumn("neighborhood",
                         __neighborhood(boundaries, neighborhoods)("latitude", "longitude"))
