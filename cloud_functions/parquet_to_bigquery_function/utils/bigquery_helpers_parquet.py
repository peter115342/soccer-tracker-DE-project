from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import logging
from typing import List, Tuple

def create_match_table_if_not_exists(client: bigquery.Client, dataset_id: str, table_id: str):
    """Creates the match table if it doesn't exist."""
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("date", "TIMESTAMP"),
            bigquery.SchemaField("venue", "STRING"),
            bigquery.SchemaField("home_team", "STRING"),
            bigquery.SchemaField("away_team", "STRING"),
            bigquery.SchemaField("score", "STRING"),
            bigquery.SchemaField("league", "STRING"),
            bigquery.SchemaField("season", "INTEGER"),
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Created table {dataset_id}.{table_id}")

def create_weather_table_if_not_exists(client: bigquery.Client, dataset_id: str, table_id: str):
    """Creates the weather table if it doesn't exist."""
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        client.get_table(table_ref)
    except NotFound:
        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("humidity", "FLOAT"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("precipitation", "FLOAT"),
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info(f"Created table {dataset_id}.{table_id}")

def load_match_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str]
) -> Tuple[int, List[str]]:
    """Load match parquet files to BigQuery with duplicate checking."""
    create_match_table_if_not_exists(client, dataset_id, table_id)
    loaded_count = 0
    processed_files = []
    
    query = f"""
    SELECT DISTINCT id 
    FROM `{dataset_id}.{table_id}`
    """
    existing_ids = {row.id for row in client.query(query).result()}
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        match_id = file_path.split('/')[-1].replace('.parquet', '')
        
        if match_id in existing_ids:
            logging.info(f"Match {match_id} already exists in BigQuery, skipping")
            continue
            
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        uri = f"gs://{bucket_name}/{file_path}"
        
        load_job = client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config
        )
        
        load_job.result()
        loaded_count += 1
        processed_files.append(uri)
        logging.info(f"Loaded match data: {uri}")
    
    return loaded_count, processed_files

def load_weather_parquet_to_bigquery(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    bucket_name: str,
    files: List[str]
) -> Tuple[int, List[str]]:
    """Load weather parquet files to BigQuery with duplicate checking."""
    create_weather_table_if_not_exists(client, dataset_id, table_id)
    loaded_count = 0
    processed_files = []
    
    query = f"""
    SELECT DISTINCT 
        CONCAT(CAST(timestamp AS STRING), location) as unique_key
    FROM `{dataset_id}.{table_id}`
    """
    existing_keys = {row.unique_key for row in client.query(query).result()}
    
    for file_path in files:
        if not file_path.endswith('.parquet'):
            continue
            
        file_key = file_path.split('/')[-1].replace('.parquet', '')
        
        if file_key in existing_keys:
            logging.info(f"Weather data {file_key} already exists in BigQuery, skipping")
            continue
            
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        uri = f"gs://{bucket_name}/{file_path}"
        
        load_job = client.load_table_from_uri(
            uri,
            f"{dataset_id}.{table_id}",
            job_config=job_config
        )
        
        load_job.result()
        loaded_count += 1
        processed_files.append(uri)
        logging.info(f"Loaded weather data: {uri}")
    
    return loaded_count, processed_files
