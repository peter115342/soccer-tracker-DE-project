import logging
from typing import List
from google.cloud import bigquery, storage
import polars as pl
import os

def load_match_parquet_to_bigquery(
    bigquery_client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    match_files: List[str],
):
    """
    Loads match Parquet files from GCS to BigQuery, adding the id from the filename and avoiding duplicates.
    Uses Polars for efficient Parquet handling.
    """
    existing_ids_query = f"SELECT DISTINCT id FROM `{bigquery_client.project}.{dataset_id}.{table_id}`"
    existing_ids = set()
    try:
        query_job = bigquery_client.query(existing_ids_query)
        existing_ids = {row.id for row in query_job}
    except Exception as e:
        logging.warning(f"Could not fetch existing IDs from BigQuery: {e}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    total_loaded = 0
    for file_name in match_files:
        if not file_name.endswith('.parquet'):
            continue

        match_id = int(os.path.basename(file_name).split('.')[0])

        if match_id in existing_ids:
            logging.info(f"Match ID {match_id} already exists in BigQuery. Skipping.")
            continue

        blob = bucket.blob(file_name)
        temp_file_name = f"/tmp/{match_id}.parquet"
        blob.download_to_filename(temp_file_name)

        try:
            df = pl.read_parquet(temp_file_name)
            
            df = df.with_columns(pl.lit(match_id).alias('id'))
            
            pandas_df = df.to_pandas()
            
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            )
            
            load_job = bigquery_client.load_table_from_dataframe(
                pandas_df,
                destination=f"{dataset_id}.{table_id}",
                job_config=job_config,
            )
            load_job.result()
            logging.info(f"Loaded match ID {match_id} into BigQuery.")
            total_loaded += 1
            
        except Exception as e:
            logging.error(f"Failed to process match ID {match_id}: {e}")
        finally:
            if os.path.exists(temp_file_name):
                os.remove(temp_file_name)

    return total_loaded
