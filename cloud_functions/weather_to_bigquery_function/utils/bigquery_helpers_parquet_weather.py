from google.cloud import bigquery
import logging
from typing import List, Tuple

def load_weather_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load weather Parquet files to BigQuery."""
    loaded_count = 0
    processed_files: List[str] = []
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    weather_ids = [
        int(file.split('/')[-1].replace('.parquet', ''))
        for file in files
        if file.endswith('.parquet')
    ]
    
    if not weather_ids:
        return loaded_count, processed_files

    query = f"""
        SELECT DISTINCT id
        FROM `{table_ref}`
        WHERE id IN UNNEST(@weather_ids)
    """
    job_config_query = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("weather_ids", "INT64", weather_ids)
        ]
    )
    
    existing_weather = {
        row.id for row in client.query(query, job_config=job_config_query).result()
    }

    for file in files:
        if not file.endswith('.parquet'):
            continue

        weather_id = int(file.split('/')[-1].replace('.parquet', ''))
        
        if weather_id in existing_weather:
            logging.info(f"Weather {weather_id} already exists in BigQuery, skipping...")
            continue

        uri = f"gs://{bucket_name}/{file}"
        try:
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()

            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded weather {weather_id} into {table_ref}")

        except Exception as e:
            error_msg = f"Error loading weather ID {weather_id} from file {file}: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)

    return loaded_count, processed_files
