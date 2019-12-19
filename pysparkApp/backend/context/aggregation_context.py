from pyspark.sql import DataFrame, SparkSession

from context.context import Context


class AggregationContext(Context):
    catalog = None

    def load_hbase(self, spark: SparkSession) -> DataFrame:
        return spark.read.options(catalog=self.catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.catalog, newtable="5").format(self._data_source_format).save()
