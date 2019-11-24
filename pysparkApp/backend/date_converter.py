from datetime import datetime

API_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
SERVICE_CASE_TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"
HISTORICAL_INCIDENT_TIME_FORMAT = ""  # TODO
MODERN_INCIDENT_TIME_FORMAT = ""  # TODO


def convert(source_date: str, source_format: str, target_format: str) -> str:
    date = datetime.strptime(source_date, source_format)
    if target_format.endswith("%f"):
        return datetime.strftime(date, target_format)[:-3]
    else:
        return datetime.strftime(date, target_format)
