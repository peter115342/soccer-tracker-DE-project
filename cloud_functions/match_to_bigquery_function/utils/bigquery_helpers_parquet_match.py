from google.cloud import bigquery
import logging
from typing import List, Tuple

def extract_match_id(filename: str) -> str:
    """Extract match ID from filename."""
    return filename.split('/')[-1].replace('.parquet', '')

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match Parquet files to BigQuery."""
    loaded_count = 0
    processed_files: List[str] = []

    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    try:
        table = client.get_table(table_ref)
        job_config.schema = table.schema 
    except Exception:
        logging.info(f"Table {table_ref} does not exist yet. Will be created with inferred schema.")

    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]
    job_config.autodetect = True

    for file in files:
        if not file.endswith('.parquet'):
            continue

        match_id = extract_match_id(file)
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
            logging.info(f"Successfully loaded match file with match_id {match_id} into {table_ref}")
            
        except Exception as e:
            logging.error(f"Error loading match file {file} into {table_ref}: {str(e)}")
            raise

    return loaded_count, processed_files
