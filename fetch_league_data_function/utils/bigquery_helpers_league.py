import os
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any

_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def insert_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> None:
    table_id: str = f'{project_id}.sports_data.{table_name}'

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,

    )

    load_job = client.load_table_from_json(
        data,
        table_id,
        job_config=job_config,
    )
    
    load_job.result()
    
    if load_job.errors:
        print(f"Errors occurred while inserting into {table_name}: {load_job.errors}")
    else:
        print(f"Successfully loaded {len(data)} rows into {table_name}")
