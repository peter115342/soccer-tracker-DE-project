from google.cloud import bigquery, storage
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
    loaded_count = 0
    processed_files: List[str] = []
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    for file in files:
        if not file.endswith('.parquet'):
            continue

        match_id = file.split('/')[-1].replace('.parquet', '')
        uri = f"gs://{bucket_name}/{file}"

        query = f"""
            SELECT COUNT(*) as count 
            FROM `{table_ref}` 
            WHERE id = CAST(@match_id AS INT64)
        """
        job_config_query = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("match_id", "STRING", match_id)
            ]
        )

        if next(client.query(query, job_config=job_config_query).result()).count > 0:
            logging.info(f"Match {match_id} already exists in BigQuery, skipping...")
            continue

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
            error_msg = f"Error loading match ID {match_id} from file {file}: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)

    return loaded_count, processed_files
