from pyspark.sql import DataFrame, SparkSession

from context.context import Context


class AggregationContext(Context):
    catalog = None

    def load_hbase(self, spark: SparkSession) -> DataFrame:
        return spark.read.options(catalog=self.catalog).format(self._data_source_format).load()

    def load_hbase_timestamp(self, spark: SparkSession, timestamp: int) -> DataFrame:
        df = spark.read.options(catalog=self.catalog).format(self._data_source_format).load()
        df = df.where(df["time"] > timestamp)
        return df

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.catalog, newtable="5").format(self._data_source_format).save()
