from google.cloud import bigquery
import logging
from typing import List, Tuple

def load_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig,
    file_type: str
) -> Tuple[int, List[str]]:
    """Load Parquet files to BigQuery, creating the table if it doesn't exist."""
    loaded_count = 0
    processed_files: List[str] = []

    uris = [f"gs://{bucket_name}/{f}" for f in files if f.endswith('.parquet')]
    if not uris:
        logging.warning("No Parquet files found to load.")
        return loaded_count, processed_files

    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    try:
        client.get_table(table_ref)
        logging.info(f"Table {table_ref} exists. Appending data.")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    except Exception:
        logging.info(f"Table {table_ref} does not exist. Creating a new table.")
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        job_config.autodetect = True
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        ]
        job_config.source_format = bigquery.SourceFormat.PARQUET

    try:
        load_job = client.load_table_from_uri(
            uris,
            table_ref,
            job_config=job_config
        )
        load_job.result()
        loaded_count = len(uris)
        processed_files.extend(uris)
        logging.info(f"Loaded {loaded_count} {file_type} file(s) into {table_ref} successfully.")
    except Exception as e:
        logging.error(f"Error loading {file_type} files into {table_ref}: {str(e)}")

    return loaded_count, processed_files

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match Parquet files to BigQuery, handling table creation and loading."""
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
    """Load weather Parquet files to BigQuery, handling table creation and loading."""
    return load_parquet_to_bigquery(
        client,
        dataset_id,
        table_id,
        bucket_name,
        files,
        job_config,
        file_type="weather"
    )
