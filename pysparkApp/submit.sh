#!/bin/bash
spark-submit \
    --master local[2] \
    --jars /backend/shc-core-1.1.3-2.4-s_2.11-jar-with-dependencies.jar,mysql-connector-java-8.0.18.jar \
    --packages org.apache.spark:spark-streaming-flume_2.11:2.4.4 \
    --py-files /backend/files.zip \
    /backend/batch/service/service_aggregator.py
    #/backend/batch/service/data_importer.py
