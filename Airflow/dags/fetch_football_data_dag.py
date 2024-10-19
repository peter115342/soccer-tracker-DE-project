from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import requests
from typing import Dict, Any, List
import os
from google.cloud import bigquery
from google.auth import default

from utils.api_helpers import fetch_all_fixtures_by_date, filter_fixtures_by_leagues, fetch_match_statistics, LEAGUE_IDS
from utils.data_processing import process_fixtures, process_match_statistics
from utils.bigquery_helpers import insert_data_into_bigquery


default_args: Dict[str, Any] = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_fixtures(**context):
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    fixtures = fetch_all_fixtures_by_date(date_str)
    filtered_fixtures = filter_fixtures_by_leagues(fixtures, LEAGUE_IDS)
    context['ti'].xcom_push(key='fixtures', value=filtered_fixtures)
    return f"Fetched {len(filtered_fixtures)} fixtures for {date_str}"

def process_fixtures_task(**context):
    fixtures = context['ti'].xcom_pull(key='fixtures', task_ids='fetch_fixtures')
    processed_fixtures = process_fixtures(fixtures)
    context['ti'].xcom_push(key='processed_fixtures', value=processed_fixtures)
    return f"Processed {len(processed_fixtures)} fixtures"

def fetch_match_statistics_task(**context):
    fixtures = context['ti'].xcom_pull(key='fixtures', task_ids='fetch_fixtures')
    all_stats = []
    for index, fixture in enumerate(fixtures):
        fixture_id = fixture['fixture']['id']
        stats = fetch_match_statistics(fixture_id, index, len(fixtures))
        all_stats.extend(stats)
    context['ti'].xcom_push(key='match_statistics', value=all_stats)
    return f"Fetched statistics for {len(all_stats)} matches"

def process_match_statistics_task(**context):
    match_statistics = context['ti'].xcom_pull(key='match_statistics', task_ids='fetch_match_statistics')
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    processed_stats = process_match_statistics(match_statistics, date_str)
    context['ti'].xcom_push(key='processed_match_statistics', value=processed_stats)
    return f"Processed statistics for {len(processed_stats)} matches"

def insert_fixtures_to_bigquery(**context):
    processed_fixtures = context['ti'].xcom_pull(key='processed_fixtures', task_ids='process_fixtures')
    insert_data_into_bigquery('fixtures', processed_fixtures)
    return f"Inserted {len(processed_fixtures)} fixtures into BigQuery"

def insert_match_statistics_to_bigquery(**context):
    processed_stats = context['ti'].xcom_pull(key='processed_match_statistics', task_ids='process_match_statistics')
    insert_data_into_bigquery('match_statistics', processed_stats)
    return f"Inserted statistics for {len(processed_stats)} matches into BigQuery"

with DAG(
    'fetch_football_data_dag',
    default_args=default_args,
    description='DAG to fetch football data and save to BigQuery',
    schedule_interval='0 23 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    fetch_fixtures_task = PythonOperator(
        task_id='fetch_fixtures',
        python_callable=fetch_fixtures,
        provide_context=True,
    )

    process_fixtures_task = PythonOperator(
        task_id='process_fixtures',
        python_callable=process_fixtures_task,
        provide_context=True,
    )

    fetch_match_statistics_task = PythonOperator(
        task_id='fetch_match_statistics',
        python_callable=fetch_match_statistics_task,
        provide_context=True,
    )

    process_match_statistics_task = PythonOperator(
        task_id='process_match_statistics',
        python_callable=process_match_statistics_task,
        provide_context=True,
    )

    insert_fixtures_to_bigquery_task = PythonOperator(
        task_id='insert_fixtures_to_bigquery',
        python_callable=insert_fixtures_to_bigquery,
        provide_context=True,
    )

    insert_match_statistics_to_bigquery_task = PythonOperator(
        task_id='insert_match_statistics_to_bigquery',
        python_callable=insert_match_statistics_to_bigquery,
        provide_context=True,
    )

    fetch_fixtures_task >> process_fixtures_task >> insert_fixtures_to_bigquery_task
    fetch_fixtures_task >> fetch_match_statistics_task >> process_match_statistics_task >> insert_match_statistics_to_bigquery_task
