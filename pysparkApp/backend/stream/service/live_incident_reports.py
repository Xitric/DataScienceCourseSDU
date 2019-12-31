from context.incident.incident_modern_context import IncidentModernContext
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext
from stream.service.live_ingester import ingest

if __name__ == "__main__":
    ingest("incident",
           IncidentModernContext(),
           IncidentRunningAggregationContext())
