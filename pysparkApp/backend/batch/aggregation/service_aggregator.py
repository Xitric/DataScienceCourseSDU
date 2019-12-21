from batch.aggregation.aggregator import aggregate
from context.service.service_running_aggregation_context import ServiceRunningAggregationContext

if __name__ == "__main__":
    aggregate("service_cases", ServiceRunningAggregationContext())
