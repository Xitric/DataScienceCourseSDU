from datetime import datetime

from geo_pyspark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession

from context.service_case_context import ServiceCaseContext

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    GeoSparkRegistrator.registerAll(spark)

    service_context = ServiceCaseContext()
    service_csv_df = service_context.load_csv(spark) \
        .limit(1000)

    service_csv_df.explain()
    service_csv_df.show(1000, True)

    # Store in HBase for further batch processing
    # service_context.save_hbase(service_csv_df)

    # seconds = int(datetime.strptime("2019-12-01", "yyyy-MM-dd").timestamp())
    # service_hbase_df = service_context.load_hbase(spark)
    # service_hbase_df = service_hbase_df.where(service_hbase_df["opened"] < seconds)
    # service_hbase_df.show(20, False)
