#!/bin/bash
spark-submit \
    --master local[2] \
    --jars /backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar \
    --packages org.apache.spark:spark-streaming-flume_2.11:2.4.4 \
    --py-files /backend/context.py,/backend/incident_modern_context.py,/backend/service_case_context.py,/backend/string_hasher.py \
    /backend/monthly_service_aggregator.py
    # /backend/live_service_cases.py
