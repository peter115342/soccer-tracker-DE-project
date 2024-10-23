from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
from typing import Dict, Any, List

default_args: Dict[str, Any] = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_football_data_dag',
    default_args=default_args,
    description='DAG to fetch football data and save to BigQuery',
    schedule_interval='0 23 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def fetch_fixtures(**context):
        from utils.api_helpers import fetch_fixtures_by_date
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        fixtures = fetch_fixtures_by_date(date_str)
        return fixtures

    @task
    def fetch_match_data(fixtures: List[Dict[str, Any]]):
        from utils.api_helpers import fetch_match_data
        match_ids = [fixture['id'] for fixture in fixtures]
        match_data_list = fetch_match_data(match_ids)
        return match_data_list

    @task
    def process_match_data(match_data_list: List[Dict[str, Any]], **context):
        from utils.data_processing import process_match_data
        execution_date = context['execution_date']
        date_str = execution_date.strftime('%Y-%m-%d')
        processed_matches = process_match_data(match_data_list, date_str)
        return processed_matches

    @task
    def insert_match_data_to_bigquery(processed_matches: List[Dict[str, Any]]):
        from utils.bigquery_helpers import insert_data_into_bigquery
        insert_data_into_bigquery('match_data', processed_matches)
        return f"Inserted data for {len(processed_matches)} matches into BigQuery"

    fixtures = fetch_fixtures()

    match_data_list = fetch_match_data(fixtures)
    processed_matches = process_match_data(match_data_list)
    insert_matches_result = insert_match_data_to_bigquery(processed_matches)

    fixtures >> match_data_list >> processed_matches >> insert_matches_result
