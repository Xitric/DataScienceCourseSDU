import os

from geo_pyspark.sql.types import GeometryType
from pyspark.sql import DataFrame, session, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from shapely.geometry import Polygon, Point

# True if the latitude and longitude is in the neighborhood represented as a Polygon
is_neighborhood_in_polygon = udf(
    lambda latitude, longitude, polygon_object:
    polygon_object.contains(Point(float(latitude), float(longitude))),
    BooleanType()
)

polygon = udf(
    lambda neighborhood_boundary_string: create_polygon(neighborhood_boundary_string), GeometryType()
)


# Create and return an array of points based on the neighborhood boundary string
def create_polygon(neighborhood_boundary_string) -> Polygon:
    neighborhood_boundary = neighborhood_boundary_string[16:-3]
    points_with_spaces = neighborhood_boundary.split(", ")
    points = []
    for point in points_with_spaces:
        coordinates = point.split(" ")
        points.append(Point(float(coordinates[1]), float(coordinates[0])))
    return Polygon([[p.x, p.y] for p in points])


# Read the neighborhood file from HDFS and create polygons based on the neighborhood boundaries
def neighborhood_boundaries(spark: SparkSession) -> DataFrame:
    sf_boundaries_file = os.environ["CORE_CONF_fs_defaultFS"] + "/datasets/SFFind_Neighborhoods.csv"
    sf_boundaries_df = spark.read.format("csv").option("header", "true").load(sf_boundaries_file)
    sf_boundaries_df = sf_boundaries_df.withColumn("the_geom", polygon(sf_boundaries_df["the_geom"]))
    sf_boundaries_df = sf_boundaries_df.select(
        sf_boundaries_df["the_geom"].alias("polygon"),
        sf_boundaries_df["name"].alias("neighborhood")
    )
    return sf_boundaries_df
