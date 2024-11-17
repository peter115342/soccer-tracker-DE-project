import polars as pl
from google.cloud import storage, bigquery
import json
from typing import List, Dict, Any
import logging

def get_json_files_from_gcs(bucket_name: str, project_id: str) -> List[Dict[str, Any]]:
    """Fetches new JSON files from the GCS bucket and returns their contents as a list."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='match_data/')
    logging.info(f"Accessing bucket: {bucket_name}")

    bq_client = bigquery.Client(project=project_id)
    processed_files_table = f"{project_id}.sports_data.processed_files"

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{processed_files_table}` (
        file_name STRING,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    bq_client.query(create_table_query).result()

    query = f"SELECT file_name FROM `{processed_files_table}`"
    processed_files = set(row.file_name for row in bq_client.query(query).result())
    logging.info(f"Retrieved {len(processed_files)} processed files from BigQuery")

    match_data = []
    new_file_names = []

    for blob in blobs:
        if blob.name.endswith('.json') and blob.name not in processed_files:
            content = json.loads(blob.download_as_string())
            match_data.append(content)
            new_file_names.append(blob.name)
        else:
            logging.info(f"Skipping already processed file: {blob.name}")

    logging.info(f"Retrieved {len(match_data)} new JSON files from GCS")
    
    if new_file_names:
        rows_to_insert = [{'file_name': name} for name in new_file_names]
        errors = bq_client.insert_rows_json(processed_files_table, rows_to_insert)
        if errors:
            logging.error(f"Failed to insert processed file names into BigQuery: {errors}")
            raise RuntimeError(f"Failed to update processed files in BigQuery: {errors}")

    return match_data

def process_match_data(match_data_list: List[Dict[str, Any]]) -> pl.DataFrame:
    """Transforms match data into a Polars DataFrame with the correct schema."""
    if not match_data_list:
        logging.info("No new matches to process")
        return pl.DataFrame()
    
    df = pl.DataFrame(match_data_list)
    
    df = df.with_columns([
        pl.col('utcDate').str.strptime(pl.Datetime, format='%Y-%m-%dT%H:%M:%SZ'),
        pl.col('lastUpdated').str.strptime(pl.Datetime, format='%Y-%m-%dT%H:%M:%SZ'),
    ])
    
    df = df.with_columns([
        pl.col('utcDate').dt.offset_by('1h').alias('cetDate')
    ])
    
    df = df.with_columns([
        pl.struct(['homeTeam']).alias('homeTeam'),
        pl.struct(['awayTeam']).alias('awayTeam'),
        pl.struct(['competition']).alias('competition'),
        
        pl.struct([
            'score.winner',
            'score.duration',
            pl.struct([
                'score.fullTime.home',
                'score.fullTime.away'
            ]).alias('fullTime'),
            pl.struct([
                'score.halfTime.home',
                'score.halfTime.away'
            ]).alias('halfTime')
        ]).alias('score')
    ])
    
    df = df.select([
        'id',
        'utcDate',
        'cetDate',
        'status',
        'matchday',
        'stage',
        'lastUpdated',
        'homeTeam',
        'awayTeam',
        'competition',
        'score'
    ])
    
    logging.info(f"Processed {len(df)} matches")
    return df

def transform_to_bigquery_rows(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Converts Polars DataFrame to BigQuery-compatible row format."""
    if df.is_empty():
        return []
    return df.to_dicts()
