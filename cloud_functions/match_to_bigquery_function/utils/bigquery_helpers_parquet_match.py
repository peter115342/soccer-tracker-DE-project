from google.cloud import bigquery, storage
import logging
from typing import List, Tuple
import polars as pl
import os

def extract_match_id(filename: str) -> str:
    """Extract match ID from filename."""
    return filename.split('/')[-1].replace('.parquet', '')

def transform_match_data(file_path: str) -> pl.DataFrame:
    """Transform match data to handle null values and type mismatches."""
    df = pl.read_parquet(file_path)
    
    df = df.with_columns([
        pl.col('referees').map_elements(lambda x: {
            'id': x.get('id'),
            'name': x.get('name'),
            'type': x.get('type'),
            'nationality': str(x.get('nationality', '')) if x.get('nationality') is not None else ''
        } if x is not None else None)
    ])
    
    return df

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load match Parquet files to BigQuery.
    
    Args:
        client: BigQuery client instance
        dataset_id: Target dataset ID
        table_id: Target table ID
        bucket_name: GCS bucket name
        files: List of files to process
        job_config: BigQuery load job configuration
        
    Returns:
        Tuple containing:
        - Number of successfully loaded files
        - List of processed file URIs
    """
    loaded_count = 0
    processed_files: List[str] = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    for file in files:
        if not file.endswith('.parquet'):
            continue

        match_id = extract_match_id(file)
        
        query = f"""
            SELECT COUNT(*) as count 
            FROM `{table_ref}` 
            WHERE id = CAST({match_id} AS INT64)
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        if next(results).count > 0:
            logging.info(f"Match {match_id} already exists in BigQuery, skipping...")
            continue

        uri = f"gs://{bucket_name}/{file}"
        temp_path = f"/tmp/{match_id}.parquet"
        transformed_path = f"/tmp/{match_id}_transformed.parquet"

        try:
            blob = bucket.blob(file)
            blob.download_to_filename(temp_path)
            
            df = transform_match_data(temp_path)
            
            df.write_parquet(transformed_path)
            
            transformed_blob = bucket.blob(f"transformed_match_data/{match_id}_transformed.parquet")
            transformed_blob.upload_from_filename(transformed_path)
            
            transformed_uri = f"gs://{bucket_name}/transformed_match_data/{match_id}_transformed.parquet"
            load_job = client.load_table_from_uri(
                transformed_uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded match file with match_id {match_id} into {table_ref}")
            
            os.remove(temp_path)
            os.remove(transformed_path)
            transformed_blob.delete()
            
        except Exception as e:
            logging.error(f"Error loading match file {file} into {table_ref}: {str(e)}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if os.path.exists(transformed_path):
                os.remove(transformed_path)
            raise

    return loaded_count, processed_files
