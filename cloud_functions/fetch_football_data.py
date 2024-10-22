
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from utils.api_helpers import fetch_fixtures_by_date, fetch_match_data
from utils.data_processing import process_match_data
from utils.bigquery_helpers import insert_data_into_bigquery

def fetch_football_data(request):
    """
    Cloud Function to fetch football match data and load into BigQuery.
    """
    date_str = request.args.get('date')
    if not date_str:
        date = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = date.strftime('%Y-%m-%d')

    fixtures = fetch_fixtures_by_date(date_str)

    if not fixtures:
        return f"No fixtures found for date {date_str}.", 200

    match_ids = [fixture['id'] for fixture in fixtures]
    match_data_list = fetch_match_data(match_ids)

    processed_matches = process_match_data(match_data_list, date_str)

    insert_data_into_bigquery('match_data', processed_matches)

    return f"Inserted data for {len(processed_matches)} matches into BigQuery.", 200
