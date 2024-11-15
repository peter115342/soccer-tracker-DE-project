import polars as pl
from google.cloud import storage, bigquery
import json
from typing import List, Dict, Any
import logging

def get_json_files_from_gcs(bucket_name: str, project_id: str) -> List[Dict[str, Any]]:
    """Fetches new JSON files from the GCS bucket and returns their contents as a list."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    bq_client = bigquery.Client(project=project_id)
    existing_matches_query = f"""
    SELECT DISTINCT match_id 
    FROM `{project_id}.sports_data.weather_data`
    """
    existing_matches = set(row.match_id for row in bq_client.query(existing_matches_query).result())
    logging.info(f"Found {len(existing_matches)} existing matches in weather_data table")

    match_ids_query = f"""
    SELECT id 
    FROM `{project_id}.sports_data.match_data`
    """
    match_ids = set(row.id for row in bq_client.query(match_ids_query).result())
    logging.info(f"Found {len(match_ids)} matches in match_data table")

    new_match_ids = match_ids - existing_matches
    weather_data = []

    for match_id in new_match_ids:
        blob_path = f'weather_data/{match_id}.json'
        blob = bucket.blob(blob_path)
        
        if blob.exists():
            content = json.loads(blob.download_as_string())
            content['match_id'] = match_id
            weather_data.append(content)
            logging.info(f"Retrieved weather data for match {match_id}")
        else:
            logging.warning(f"No weather data file found for match {match_id}")

    logging.info(f"Retrieved {len(weather_data)} new weather data files for processing")
    return weather_data

def transform_weather_data(weather_data_list: List[Dict[str, Any]], project_id: str) -> pl.DataFrame:
    """Transforms weather data into a Polars DataFrame with the correct schema."""
    if not weather_data_list:
        logging.info("No new weather data to process")
        return pl.DataFrame()

    bq_client = bigquery.Client(project=project_id)
    match_times_query = f"""
    SELECT id, utcDate 
    FROM `{project_id}.sports_data.match_data`
    """
    query_results = bq_client.query(match_times_query).result()
    rows = [dict(row.items()) for row in query_results]
    match_times_df = pl.DataFrame(rows)

    processed_records = []

    for weather_data in weather_data_list:
        match_id = weather_data['match_id']
        
        match_time_series = match_times_df.filter(pl.col('id') == match_id).select('utcDate')
        
        if match_time_series.is_empty():
            logging.warning(f"No match time found for match_id {match_id}")
            continue

        match_time = match_time_series.item()
        match_date = match_time.strftime('%Y-%m-%d')
        match_hour = match_time.hour

        match_hour_index = None
        next_hour_index = None
        for i, timestamp in enumerate(weather_data['hourly']['time']):
            if timestamp.startswith(match_date):
                hour = int(timestamp.split('T')[1].split(':')[0])
                if hour == match_hour:
                    match_hour_index = i
                    next_hour_index = i + 1 if i + 1 < len(weather_data['hourly']['time']) else None
                    break

        if match_hour_index is None or next_hour_index is None:
            logging.warning(f"Unable to get both match hour and next hour for match {match_id} at time {match_time}")
            continue

        record = {
            'match_id': match_id,
            'lat': weather_data['latitude'],
            'lon': weather_data['longitude'],
            'timestamp': weather_data['hourly']['time'][match_hour_index],
            'temperature_2m': (weather_data['hourly']['temperature_2m'][match_hour_index] + 
                             weather_data['hourly']['temperature_2m'][next_hour_index]) / 2,
            'relativehumidity_2m': (weather_data['hourly']['relativehumidity_2m'][match_hour_index] + 
                                   weather_data['hourly']['relativehumidity_2m'][next_hour_index]) / 2,
            'dewpoint_2m': (weather_data['hourly']['dewpoint_2m'][match_hour_index] + 
                           weather_data['hourly']['dewpoint_2m'][next_hour_index]) / 2,
            'apparent_temperature': (weather_data['hourly']['apparent_temperature'][match_hour_index] + 
                                   weather_data['hourly']['apparent_temperature'][next_hour_index]) / 2,
            'precipitation': (weather_data['hourly']['precipitation'][match_hour_index] + 
                            weather_data['hourly']['precipitation'][next_hour_index]) / 2,
            'rain': (weather_data['hourly']['rain'][match_hour_index] + 
                    weather_data['hourly']['rain'][next_hour_index]) / 2,
            'snowfall': (weather_data['hourly']['snowfall'][match_hour_index] + 
                        weather_data['hourly']['snowfall'][next_hour_index]) / 2,
            'snow_depth': (weather_data['hourly']['snow_depth'][match_hour_index] + 
                          weather_data['hourly']['snow_depth'][next_hour_index]) / 2,
            'weathercode': round((weather_data['hourly']['weathercode'][match_hour_index] + 
                                weather_data['hourly']['weathercode'][next_hour_index]) / 2),
            'pressure_msl': (weather_data['hourly']['pressure_msl'][match_hour_index] + 
                           weather_data['hourly']['pressure_msl'][next_hour_index]) / 2,
            'cloudcover': (weather_data['hourly']['cloudcover'][match_hour_index] + 
                         weather_data['hourly']['cloudcover'][next_hour_index]) / 2,
            'windspeed_10m': (weather_data['hourly']['windspeed_10m'][match_hour_index] + 
                            weather_data['hourly']['windspeed_10m'][next_hour_index]) / 2,
            'winddirection_10m': (weather_data['hourly']['winddirection_10m'][match_hour_index] + 
                                 weather_data['hourly']['winddirection_10m'][next_hour_index]) / 2,
            'windgusts_10m': (weather_data['hourly']['windgusts_10m'][match_hour_index] + 
                             weather_data['hourly']['windgusts_10m'][next_hour_index]) / 2
        }
        processed_records.append(record)

    df = pl.DataFrame(processed_records)
    logging.info(f"Processed {len(df)} weather records with averaged values over match duration")
    return df

def transform_to_bigquery_rows(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Converts Polars DataFrame to BigQuery-compatible row format."""
    if df.is_empty():
        return []
    return df.to_dicts()
