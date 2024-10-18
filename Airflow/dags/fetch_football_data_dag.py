from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
from typing import Dict, Any
import os
from dotenv import load_dotenv

default_args: Dict[str, Any] = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': "pmuzslay8@gmail.com",
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def invoke_cloud_function(**context: Dict[str, Any]) -> str:
    load_dotenv()

    token = os.getenv('GCLOUD_AUTH')

    if not token:
        raise ValueError("GCLOUD_AUTH not found in .env file")

    url: str = 'https://fetch-sports-data-279923397259.europe-central2.run.app'

    headers = {
        'Authorization': f'Bearer {token}'
    }

    response: requests.Response = requests.get(url, headers=headers)
    response.raise_for_status()
    return "Task completed successfully"

with DAG(
    'fetch_sports_data_dag',
    default_args=default_args,
    description='DAG to invoke the fetch_sports_data Cloud Function',
    schedule_interval='0 23 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    invoke_function = PythonOperator(
        task_id='invoke_fetch_sports_data_function',
        python_callable=invoke_cloud_function,
        provide_context=True,
    )
