from google.cloud import bigquery
import logging
from typing import List, Tuple

def extract_weather_id(filename: str) -> str:
    """Extract weather ID from filename."""
    return filename.split('/')[-1].replace('.parquet', '')

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

    for file in files:
        if not file.endswith('.parquet'):
            continue

        weather_id = extract_weather_id(file)
        uri = f"gs://{bucket_name}/{file}"
        table_ref = f"{client.project}.{dataset_id}.{table_id}"
        
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
        ]
        job_config.destination_table_description = f"Weather data loaded with weather_id: {weather_id}"

        try:
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded weather file with weather_id {weather_id} into {table_ref}")
            
        except Exception as e:
            logging.error(f"Error loading weather file {file} into {table_ref}: {str(e)}")
            raise

    return loaded_count, processed_files
