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
    """Load match Parquet files to BigQuery."""
    loaded_count = 0
    processed_files: List[str] = []
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    for file in files:
        if not file.endswith('.parquet'):
            continue

        match_id = file.split('/')[-1].replace('.parquet', '')

        query = f"""
            SELECT COUNT(*) as count 
            FROM `{table_ref}` 
            WHERE id = CAST({match_id} AS INT64)
        """

        if next(client.query(query).result()).count > 0:
            logging.info(f"Match {match_id} already exists in BigQuery, skipping...")
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
            logging.info(f"Successfully loaded match {match_id} into {table_ref}")

        except Exception as e:
            logging.error(f"Error loading match file {file}: {str(e)}")
            raise

    return loaded_count, processed_files
