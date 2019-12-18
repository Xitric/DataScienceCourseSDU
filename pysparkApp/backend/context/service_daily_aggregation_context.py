from context.service_aggregation_context import ServiceAggregationContext


class ServiceDailyAggregationContext(ServiceAggregationContext):
    catalog = ''.join("""{
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