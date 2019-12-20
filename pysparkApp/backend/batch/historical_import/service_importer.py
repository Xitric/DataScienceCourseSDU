from batch.historical_import.data_importer import import_data
from context.service.service_case_context import ServiceCaseContext
from context.service.service_running_aggregation_context import ServiceRunningAggregationContext
from util.spark_session_utils import get_spark_session_instance

# Load historical data about 311 Service cases from csv files in HDFS
if __name__ == "__main__":
    import_data(ServiceCaseContext(),
                get_spark_session_instance(),
                ServiceRunningAggregationContext())
