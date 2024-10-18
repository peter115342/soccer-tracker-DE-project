FROM python:3.12-slim-bullseye

ENV AIRFLOW_HOME=/app/airflow
ENV PYTHONPATH=/app
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__DAGS_FOLDER=/app/airflow/dags

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements_airflow.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
RUN pip install apache-airflow-providers-google

RUN mkdir -p /app/airflow/dags /app/airflow/logs /app/airflow/plugins
RUN chmod -R 755 /app/airflow/dags

WORKDIR /app

COPY download_dags.py /app/download_dags.py
COPY start_airflow.sh /app/start_airflow.sh
RUN chmod +x /app/start_airflow.sh

CMD ["/bin/bash", "-c", "python /app/download_dags.py && /app/start_airflow.sh"]
