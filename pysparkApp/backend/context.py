from pyspark.sql import SparkSession, DataFrame


class Context:
    _data_source_format = "org.apache.spark.sql.execution.datasources.hbase"

    def load_csv(self, session: SparkSession) -> DataFrame:
        pass

    def load_hbase(self) -> DataFrame:
        pass

    def save_hbase(self, df: DataFrame):
        pass
