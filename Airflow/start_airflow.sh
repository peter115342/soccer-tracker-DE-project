#!/bin/bash

set -e

airflow db init

airflow users list | grep -q 'admin' || \
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin


echo "Starting Airflow webserver on port $PORT"


airflow scheduler & airflow webserver --port $PORT --host 0.0.0.0
