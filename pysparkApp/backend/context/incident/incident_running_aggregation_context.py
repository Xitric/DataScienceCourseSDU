from context.aggregation_context import AggregationContext


class IncidentRunningAggregationContext(AggregationContext):
    catalog = ''.join("""{
            "table":{"namespace":"default", "name":"running_incident_aggregates"},
            "rowkey":"key_neighborhood:key_incident_category:key_time",
            "columns":{
                "neighborhood_id":{"cf":"rowkey", "col":"key_neighborhood", "type":"int"},
                "incident_category_id":{"cf":"rowkey", "col":"key_incident_category", "type":"int"},
                "time":{"cf":"rowkey", "col":"key_time", "type":"int"},

                "neighborhood":{"cf":"agg", "col":"neighborhood", "type":"string"},
                "incident_category":{"cf":"agg", "col":"incident_category", "type":"string"},
                "count":{"cf":"agg", "col":"count", "type":"int"}
            }
        }""".split())
