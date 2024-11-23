from google.cloud import bigquery
import logging
from typing import List, Tuple

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match parquet files to BigQuery with auto schema detection."""
    loaded_count = 0
    processed_files: List[str] = []

    first_file = next((f for f in files if f.endswith('.parquet')), None)
    if not first_file:
        return loaded_count, processed_files

    table_ref = f"{dataset_id}.{table_id}"
    
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    try:
        client.get_table(table_ref)
    except Exception:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
    
    uris = [f"gs://{bucket_name}/{f}" for f in files if f.endswith('.parquet')]
    if not uris:
        return loaded_count, processed_files
        
    try:
        load_job = client.load_table_from_uri(
            uris,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        loaded_count = len(uris)
        processed_files.extend(uris)
        logging.info(f"Loaded {loaded_count} match files successfully")
    except Exception as e:
        logging.error(f"Error loading match files: {str(e)}")
    
    return loaded_count, processed_files

def load_weather_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load weather parquet files to BigQuery with auto schema detection."""
    loaded_count = 0
    processed_files: List[str] = []

    first_file = next((f for f in files if f.endswith('.parquet')), None)
    if not first_file:
        return loaded_count, processed_files

    table_ref = f"{dataset_id}.{table_id}"
    
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    try:
        client.get_table(table_ref)
    except Exception:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
    
    uris = [f"gs://{bucket_name}/{f}" for f in files if f.endswith('.parquet')]
    if not uris:
        return loaded_count, processed_files
        
    try:
        load_job = client.load_table_from_uri(
            uris,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        loaded_count = len(uris)
        processed_files.extend(uris)
        logging.info(f"Loaded {loaded_count} weather files successfully")
    except Exception as e:
        logging.error(f"Error loading weather files: {str(e)}")
    
    return loaded_count, processed_files
