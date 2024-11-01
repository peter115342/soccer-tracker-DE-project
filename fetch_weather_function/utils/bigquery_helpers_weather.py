import os
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any
import logging

_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def insert_weather_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    table_id: str = f'{project_id}.sports_data.{table_name}'

    if not data:
        logging.info("No weather data to insert.")
        return {'inserted_count': 0}

    errors = client.insert_rows_json(table_id, data)
    if errors:
        logging.error(f"Errors occurred while inserting into {table_name}: {errors}")
        raise RuntimeError(f"Failed to insert data into BigQuery: {errors}")
    else:
        logging.info(f"Successfully inserted {len(data)} rows into {table_name}.")

    return {'inserted_count': len(data)}
