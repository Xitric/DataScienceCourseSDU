from pyspark.sql import DataFrame, SparkSession

from context.context import Context


class ServiceAggregationContext(Context):
    __catalog = ''.join("""{
            "table":{"namespace":"default", "name":"daily_service_aggregates"},
            "rowkey":"key_neighborhood:key_category:key_time",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "category_id":{"cf":"rowkey", "col":"key_category", "type":"int"},
                "time":{"cf":"rowkey", "col":"key_time", "type":"int"},

                "neighborhood":{"cf":"agg", "col":"neighborhood", "type":"string"},
                "category":{"cf":"agg", "col":"category", "type":"string"},
                "count":{"cf":"agg", "col":"count", "type":"int"}
            }
        }""".split())

    def load_hbase(self, spark: SparkSession) -> DataFrame:
        return spark.read.options(catalog=self.__catalog).format(self._data_source_format).load()

    def save_hbase(self, df: DataFrame):
        df.write.options(catalog=self.__catalog, newtable="5").format(self._data_source_format).save()
