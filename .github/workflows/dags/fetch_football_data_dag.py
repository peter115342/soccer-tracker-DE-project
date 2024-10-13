from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datetime import timedelta
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def invoke_cloud_function(**context):
    url = 'https://fetch-sports-data-279923397259.europe-central2.run.app'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Failed to invoke Cloud Function: {response.text}')

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
