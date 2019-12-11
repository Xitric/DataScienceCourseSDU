#!/bin/bash
spark-submit --master yarn --deploy-mode cluster --executor-memory 1g --num-executors 1 /backend/test_file.py