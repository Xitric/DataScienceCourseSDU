from batch.aggregation.aggregator import aggregate
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext

if __name__ == "__main__":
    aggregate("incident_cases", IncidentRunningAggregationContext())
