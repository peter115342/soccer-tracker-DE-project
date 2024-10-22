from typing import List, Dict, Any
from flask import Request
from datetime import datetime

from utils.api_helpers import fetch_fixtures_by_date, fetch_match_data
from utils.data_processing import process_match_data
from utils.bigquery_helpers import insert_data_into_bigquery

def fetch_football_data(request: Request):
    """
    Cloud Function to fetch football match data and load into BigQuery.
    """
    date_str = get_date_str(request)
    fixtures = fetch_fixtures(date_str)
    
    if not fixtures:
        return f"No fixtures found for date {date_str}.", 200

    match_data_list = fetch_matches(fixtures)
    processed_matches = process_matches(match_data_list, date_str)
    result = insert_matches(processed_matches)
    return result, 200

def get_date_str(request: Request) -> str:
    request_args = request.args
    if request_args and 'date' in request_args:
        date_str = request_args['date']
    else:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
    return date_str

def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    fixtures = fetch_fixtures_by_date(date_str)
    return fixtures

def fetch_matches(fixtures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    match_ids = [fixture['id'] for fixture in fixtures]
    match_data_list = fetch_match_data(match_ids)
    return match_data_list

def process_matches(match_data_list: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
    processed_matches = process_match_data(match_data_list, date_str)
    return processed_matches

def insert_matches(processed_matches: List[Dict[str, Any]]) -> str:
    insert_data_into_bigquery('match_data', processed_matches)
    return f"Inserted data for {len(processed_matches)} matches into BigQuery."
