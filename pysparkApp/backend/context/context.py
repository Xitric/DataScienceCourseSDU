from pyspark.sql import SparkSession, DataFrame
from pyspark.streaming import StreamingContext, DStream


class Context:
    _data_source_format = "org.apache.spark.sql.execution.datasources.hbase"

    def load_csv(self, spark: SparkSession) -> DataFrame:
        pass

    def load_flume(self, ssc: StreamingContext) -> DStream:
        pass

    def load_hbase(self, spark: SparkSession) -> DataFrame:
        pass

    def load_hbase_timestamp(self, spark: SparkSession, timestamp: int) -> DataFrame:
        pass

    def save_hbase(self, df: DataFrame):
        pass

