from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

def calculateKDist(df):
    vecAssembler = VectorAssembler(inputCols=categories, outputCol="features")
    new_df = vecAssembler.transform(df)
    # new_df.select("neighborhood", "features").show(10)

    evaluator = ClusteringEvaluator()

    for i in range(2, 15):
        kmeans = KMeans(k=i, seed=1, maxIter=50)  # 2 clusters here
        model = kmeans.fit(new_df.select('features'))

        transformed = model.transform(new_df)
        # transformed.select("neighborhood", "features", "prediction").sort("prediction", ascending=False).show(34)

        # summary = model.summary
        # 'cluster', 'clusterSizes', 'featuresCol', 'k', 'numIter', 'predictionCol', 'predictions', 'trainingCost'
        # print(dir(summary))
        # print(f'{summary.k},\t {summary.trainingCost},\t {summary.clusterSizes}, ')
        print(f'{model.summary.k},\t{evaluator.evaluate(transformed)}')


if __name__ == "__main__":
    categories = ["Abandoned Vehicle", "Blocked Street or SideWalk",
        "Catch Basin Maintenance", "Color Curb", "DPW Volunteer Programs",
        "Damaged Property", "Encampments", "Graffiti", "Homeless Concerns",
        "Illegal Postings", "Litter Receptacles", "MUNI Feedback",
        "Noise Report", "Parking Enforcement", "Rec and Park Requests",
        "Residential Building Request", "SFHA Requests", "Sewer Issues",
        "Sidewalk or Curb", "Sign Repair", "Street Defects",
        "Street and Sidewalk Cleaning", "Streetlights",
        "Temporary Sign Request", "Tree Maintenance"]

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_monthly',
        user='spark',
        password='P18YtrJj8q6ioevT').load()

    df = df.na.fill(0)

    vecAssembler = VectorAssembler(inputCols=categories, outputCol="features")
    new_df = vecAssembler.transform(df)
    # new_df.select("neighborhood", "features").show(10)

    kmeans = KMeans(k=6, seed=1, maxIter=50)  # 2 clusters here
    model = kmeans.fit(new_df.select('features'))

    df_trans = model.transform(new_df)
    df_cluster = df_trans.select("neighborhood", \
        df_trans["prediction"].alias("cluster"))
        
    # df_cluster.show(35)
    # df_cluster.sort("cluster", ascending=False).show(35)

    # Save to MySQL
    df_cluster.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/analysis_results',
        driver='com.mysql.jdbc.Driver',
        dbtable='service_cases_monthly_kmeans',
        user='spark',
        password='P18YtrJj8q6ioevT').mode('overwrite').save()
