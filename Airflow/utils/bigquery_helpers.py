import os
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any, Optional

_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def insert_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> None:
    table_id: str = f'{project_id}.sports_data.{table_name}'
    errors: Optional[List[Dict[str, Any]]] = client.insert_rows_json(table_id, data)
    if errors:
        print(f"Errors occurred while inserting into {table_name}: {errors}")
        for error in errors:
            print(error)
    else:
        print(f"Data inserted into {table_name} successfully.")
