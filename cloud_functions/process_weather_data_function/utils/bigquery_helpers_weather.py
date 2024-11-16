import os
import logging
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any, Set

"""Initializes BigQuery client with project configuration."""
_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def get_existing_weather_records(table_name: str) -> Set[str]:
    """Retrieves existing weather records based on match_id and timestamp combination."""
    table_id: str = f'{project_id}.sports_data.{table_name}'
    query = f"SELECT DISTINCT match_id, timestamp FROM `{table_id}`"
    query_job = client.query(query)
    results = query_job.result()
    existing_records = set()
    for row in results:
        existing_records.add(f"{row.match_id}_{row.timestamp}")
    return existing_records

def insert_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    """Inserts new weather records into BigQuery, skipping existing records."""
    table_id: str = f'{project_id}.sports_data.{table_name}'

    existing_records = get_existing_weather_records(table_name)
    logging.info(f"Found {len(existing_records)} existing weather records in {table_name}.")

    new_data = [
        record for record in data 
        if f"{record['match_id']}_{record['timestamp']}" not in existing_records
    ]

    skipped_count = len(data) - len(new_data)
    logging.info(f"{len(new_data)} new weather records to insert into {table_name}.")
    logging.info(f"Skipped {skipped_count} records that already exist.")

    if not new_data:
        logging.info("No new weather records to insert.")
        return {'inserted_count': 0, 'skipped_count': skipped_count}

    errors = client.insert_rows_json(table_id, new_data)
    if errors:
        logging.error(f"Errors occurred while inserting into {table_name}: {errors}")
        raise RuntimeError(f"Failed to insert data into BigQuery: {errors}")
    else:
        logging.info(f"Successfully inserted {len(new_data)} rows into {table_name}.")

    return {'inserted_count': len(new_data), 'skipped_count': skipped_count}
