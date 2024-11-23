from google.cloud import bigquery, storage
import logging
from typing import List, Tuple
import polars as pl
import os

def extract_weather_id(filename: str) -> str:
    """Extract weather ID from filename."""
    return filename.split('/')[-1].replace('.parquet', '')

def transform_weather_data(file_path: str) -> pl.DataFrame:
    """Transform weather data to handle null values and type mismatches."""
    df = pl.read_parquet(file_path)
    return df

def load_weather_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str],
    job_config: bigquery.LoadJobConfig
) -> Tuple[int, List[str]]:
    """Load weather Parquet files to BigQuery."""
    loaded_count = 0
    processed_files: List[str] = []
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    for file in files:
        if not file.endswith('.parquet'):
            continue

        weather_id = extract_weather_id(file)
        
        query = f"""
            SELECT COUNT(*) as count 
            FROM `{table_ref}` 
            WHERE id = '{weather_id}'
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        if next(results).count > 0:
            logging.info(f"Weather {weather_id} already exists in BigQuery, skipping...")
            continue

        uri = f"gs://{bucket_name}/{file}"
        
        try:
            blob = bucket.blob(file)
            temp_path = f"/tmp/{weather_id}.parquet"
            blob.download_to_filename(temp_path)
            
            df = transform_weather_data(temp_path)
            
            transformed_path = f"/tmp/{weather_id}_transformed.parquet"
            df.write_parquet(transformed_path)
            
            transformed_blob = bucket.blob(f"transformed_weather_data/{weather_id}_transformed.parquet")
            transformed_blob.upload_from_filename(transformed_path)
            
            transformed_uri = f"gs://{bucket_name}/transformed_weather_data/{weather_id}_transformed.parquet"
            load_job = client.load_table_from_uri(
                transformed_uri,
                table_ref,
                job_config=job_config
            )
            load_job.result()
            
            loaded_count += 1
            processed_files.append(uri)
            logging.info(f"Successfully loaded weather file with weather_id {weather_id} into {table_ref}")
            
            os.remove(temp_path)
            os.remove(transformed_path)
            transformed_blob.delete()
            
        except Exception as e:
            logging.error(f"Error loading weather file {file} into {table_ref}: {str(e)}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            if os.path.exists(transformed_path):
                os.remove(transformed_path)
            raise

    return loaded_count, processed_files
