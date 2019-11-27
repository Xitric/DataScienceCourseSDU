from pyspark.sql import SparkSession, DataFrame


class Context:
    _data_source_format = "org.apache.spark.sql.execution.datasources.hbase"

    def __init__(self, session: SparkSession):
        self.spark = session

    def load_csv(self) -> DataFrame:
        pass

    def load_hbase(self) -> DataFrame:
        pass

    def save_hbase(self, df: DataFrame):
        pass
