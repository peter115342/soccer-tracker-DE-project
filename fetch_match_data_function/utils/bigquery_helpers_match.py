import os
from google.cloud import bigquery
from google.auth import default
from typing import List, Dict, Any, Set

_, project_id = default()

if project_id is None:
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

if not isinstance(project_id, str):
    raise ValueError("Project ID must be a string. Please set GOOGLE_CLOUD_PROJECT environment variable.")

client: bigquery.Client = bigquery.Client(project=project_id)

def get_existing_match_ids(table_name: str) -> Set[str]:
    table_id: str = f'{project_id}.sports_data.{table_name}'
    query = f"SELECT DISTINCT id FROM `{table_id}`"
    query_job = client.query(query)
    results = query_job.result()
    existing_ids = set()
    for row in results:
        existing_ids.add(str(row.id))
    return existing_ids

def insert_data_into_bigquery(table_name: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
    table_id: str = f'{project_id}.sports_data.{table_name}'

    # Fetch existing match IDs
    existing_ids = get_existing_match_ids(table_name)
    print(f"Found {len(existing_ids)} existing match IDs in {table_name}.")

    # Filter out matches that already exist
    new_data = [match for match in data if str(match['id']) not in existing_ids]
    skipped_count = len(data) - len(new_data)
    print(f"{len(new_data)} new matches to insert into {table_name}.")
    print(f"Skipped {skipped_count} matches that already exist.")

    if not new_data:
        print("No new matches to insert.")
        return {'inserted_count': 0, 'skipped_count': skipped_count}

    errors = client.insert_rows_json(table_id, new_data)
    if errors:
        print(f"Errors occurred while inserting into {table_name}: {errors}")
        raise RuntimeError(f"Failed to insert data into BigQuery: {errors}")
    else:
        print(f"Successfully inserted {len(new_data)} rows into {table_name}.")

    return {'inserted_count': len(new_data), 'skipped_count': skipped_count}
