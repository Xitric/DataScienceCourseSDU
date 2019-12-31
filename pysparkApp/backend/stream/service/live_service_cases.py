from context.service.service_case_context import ServiceCaseContext
from context.service.service_running_aggregation_context import ServiceRunningAggregationContext
from stream.service.live_ingester import ingest

if __name__ == "__main__":
    ingest("service",
           "vw6y-z8j6",
           ServiceCaseContext(),
           ServiceRunningAggregationContext())
