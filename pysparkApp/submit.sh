#!/bin/bash
spark-submit --py-files /backend/incident_modern_context.py,/backend/context.py /backend/data_importer.py