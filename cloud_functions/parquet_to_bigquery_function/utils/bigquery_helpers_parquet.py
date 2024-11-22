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
    processed_files = []
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        uri = f"gs://{bucket_name}/{file_path}"
        
        # Let BigQuery auto-detect schema from Parquet
        job_config.autodetect = True
        
        load_job = client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config
        )
        
        load_job.result()
        loaded_count += 1
        processed_files.append(uri)
        logging.info(f"Loaded match data: {uri}")
    
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
    processed_files = []
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        uri = f"gs://{bucket_name}/{file_path}"
        
        # Let BigQuery auto-detect schema from Parquet
        job_config.autodetect = True
        
        load_job = client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config
        )
        
        load_job.result()
        loaded_count += 1
        processed_files.append(uri)
        logging.info(f"Loaded weather data: {uri}")
    
    return loaded_count, processed_files
