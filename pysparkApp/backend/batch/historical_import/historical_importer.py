from batch.historical_import.data_importer import import_data
from context.incident.incident_historical_context import IncidentHistoricalContext
from context.incident.incident_running_aggregation_context import IncidentRunningAggregationContext
from util.spark_session_utils import get_spark_session_instance

# Load historical data about old incident reports from csv files in HDFS
if __name__ == "__main__":
    import_data(IncidentHistoricalContext(),
                get_spark_session_instance(),
                IncidentRunningAggregationContext())
