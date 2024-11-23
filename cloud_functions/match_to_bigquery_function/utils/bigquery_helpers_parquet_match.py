from google.cloud import bigquery, storage
import logging
from typing import List, Tuple
import polars as pl

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match Parquet files directly to BigQuery."""
    loaded_count = 0
    processed_files: List[str] = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
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

        blob = bucket.blob(file)
        temp_path = f"/tmp/{match_id}.parquet"
        
        try:
            blob.download_to_filename(temp_path)
            df = pl.read_parquet(temp_path)
            
            if 'referees' in df.columns:
                df = df.with_columns([
                    pl.col('referees').map_elements(lambda x: {
                        **x,
                        'nationality': str(x['nationality']) if x and x.get('nationality') is not None else None
                    } if x else None)
                ])
            
            transformed_path = f"/tmp/{match_id}_transformed.parquet"
            df.write_parquet(transformed_path)
            
            transformed_blob = bucket.blob(f"transformed/{match_id}_transformed.parquet")
            transformed_blob.upload_from_filename(transformed_path)
            
            uri = f"gs://{bucket_name}/transformed/{match_id}_transformed.parquet"
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded match {match_id} into {table_ref}")
            
            # Cleanup
            transformed_blob.delete()
            
        except Exception as e:
            logging.error(f"Error loading match file {file}: {str(e)}")
            raise

    return loaded_count, processed_files
