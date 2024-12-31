import os
import logging
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any

"""Initializes BigQuery client with project configuration."""
_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def insert_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> None:
    """Updates or inserts league data into the specified BigQuery table."""
    table_id: str = f'{project_id}.sports_data_eu.{table_name}'
    
    new_ids = [str(item['id']) for item in data]
    id_list = ', '.join([f"'{id}'" for id in new_ids])
    
    delete_query = f"DELETE FROM `{table_id}` WHERE CAST(id AS STRING) IN ({id_list})"
    delete_job = client.query(delete_query)
    delete_job.result()
    logging.info(f"Deleted existing records with matching IDs from {table_name}")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(
        data,
        table_id,
        job_config=job_config,
    )
    
    load_job.result()
    
    if load_job.errors:
        logging.error(f"Errors occurred while inserting into {table_name}: {load_job.errors}")
    else:
        logging.info(f"Successfully loaded {len(data)} rows into {table_name}")
