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
    
    first_file = next((f for f in files if f.endswith('.parquet')), None)
    if not first_file:
        return loaded_count, processed_files

    table_ref = f"{dataset_id}.{table_id}"
    
    try:
        client.get_table(table_ref)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    except:  # noqa: E722
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        uri = f"gs://{bucket_name}/{file_path}"
        
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        
        try:
            load_job.result()
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Loaded match data: {uri}")
        except Exception as e:
            logging.error(f"Error loading {uri}: {str(e)}")
            continue
    
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
    
    first_file = next((f for f in files if f.endswith('.parquet')), None)
    if not first_file:
        return loaded_count, processed_files

    table_ref = f"{dataset_id}.{table_id}"
    
    try:
        client.get_table(table_ref)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    except:  # noqa: E722
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        uri = f"gs://{bucket_name}/{file_path}"
        
        load_job = client.load_table_from_uri(
            uri,
            table_ref,
            job_config=job_config
        )
        
        try:
            load_job.result()
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Loaded weather data: {uri}")
        except Exception as e:
            logging.error(f"Error loading {uri}: {str(e)}")
            continue
    
    return loaded_count, processed_files
