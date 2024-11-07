import polars as pl
from google.cloud import storage
import json
from typing import List, Dict, Any
import logging

def get_json_files_from_gcs(bucket_name: str, project_id: str) -> List[Dict[str, Any]]:
    """Fetches all JSON files from the GCS bucket and returns their contents as a list."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix='match_data/')
    
    match_data = []
    for blob in blobs:
        if blob.name.endswith('.json'):
            content = json.loads(blob.download_as_string())
            match_data.append(content)
    
    logging.info(f"Retrieved {len(match_data)} JSON files from GCS")
    return match_data

def process_match_data(match_data_list: List[Dict[str, Any]]) -> pl.DataFrame:
    """Transforms match data into a Polars DataFrame with the correct schema."""
    
    df = pl.DataFrame(match_data_list)
    
    df = df.with_columns([
        pl.col('utcDate').str.strptime(pl.Datetime,  format='%Y-%m-%d'),
        pl.col('lastUpdated').str.strptime(pl.Datetime,  format='%Y-%m-%d'),
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
    return df.to_dicts()
