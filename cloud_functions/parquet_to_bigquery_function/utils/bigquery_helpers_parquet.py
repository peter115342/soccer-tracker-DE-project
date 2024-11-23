from google.cloud import bigquery
import logging
from typing import List, Tuple

def extract_match_id(filename: str) -> str:
    """Extract match ID from filename."""
    return filename.split('/')[-1].replace('.parquet', '')

def load_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig,
    file_type: str
) -> Tuple[int, List[str]]:
    """Load Parquet files to BigQuery table."""
    loaded_count = 0
    processed_files: List[str] = []

    uris = [f"gs://{bucket_name}/{f}" for f in files if f.endswith('.parquet')]
    if not uris:
        logging.warning("No Parquet files found to load.")
        return loaded_count, processed_files

    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]

    # Add match_id column from filename
    for file in files:
        match_id = extract_match_id(file)
        job_config.query_parameters = [
            bigquery.ScalarQueryParameter("match_id", "STRING", match_id)
        ]
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
            logging.info(f"Successfully loaded {file_type} file with match_id {match_id} into {table_ref}")
            
        except Exception as e:
            logging.error(f"Error loading {file_type} file {file} into {table_ref}: {str(e)}")
            raise

    return loaded_count, processed_files

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match Parquet files to BigQuery."""
    return load_parquet_to_bigquery(
        client,
        dataset_id,
        table_id,
        bucket_name,
        files,
        job_config,
        file_type="match"
    )

def load_weather_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load weather Parquet files to BigQuery."""
    return load_parquet_to_bigquery(
        client,
        dataset_id,
        table_id,
        bucket_name,
        files,
        job_config,
        file_type="weather"
    )
