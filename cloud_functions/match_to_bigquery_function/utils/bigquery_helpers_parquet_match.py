from google.cloud import bigquery
import logging
from typing import List

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    match_files: List[str],
    job_config: bigquery.LoadJobConfig
) -> int:
    """
    Load match parquet files to BigQuery while preventing duplicates and including IDs from filenames.
    Returns:
        int: Number of successfully loaded matches
    """
    successfully_loaded = 0
    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    query = f"""
    SELECT DISTINCT id 
    FROM `{table_ref}`
    """
    existing_ids = {row.id for row in client.query(query).result()}

    for file_path in match_files:
        if not file_path.endswith('.parquet') or file_path == 'match_data_parquet/':
            continue

        try:
            match_id = int(file_path.split('/')[-1].replace('.parquet', ''))
        except ValueError:
            logging.warning(f"Skipping file with invalid ID format: {file_path}")
            continue

        if match_id in existing_ids:
            logging.info(f"Skipping existing match ID: {match_id}")
            continue

        temp_table_id = f"{table_id}_temp_{match_id}"
        temp_table_ref = f"{client.project}.{dataset_id}.{temp_table_id}"

        uri = f"gs://{bucket_name}/{file_path}"
        
        try:
            load_job = client.load_table_from_uri(
                uri,
                f"{client.project}.{dataset_id}.{temp_table_id}",
                job_config=job_config
            )
            load_job.result()

            insert_query = f"""
            INSERT INTO `{table_ref}`
            SELECT * FROM `{temp_table_ref}`
            WHERE id = {match_id}
            """
            
            insert_job = client.query(insert_query)
            insert_job.result()
            
            successfully_loaded += 1
            logging.info(f"Successfully loaded match ID: {match_id}")

        except Exception as e:
            logging.error(f"Error loading match ID {match_id}: {str(e)}")
            continue
            
        finally:
            client.delete_table(temp_table_ref, not_found_ok=True)

    return successfully_loaded
