import os
import json
import logging
import time
from typing import List, Dict, Any
import requests
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get('API_FOOTBALL_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.environ.get('BUCKET_NAME')

COMPETITION_CODES = ['PL', 'BL1', 'PD', 'SA', 'FL1']
REQUESTS_PER_MINUTE = 10
REQUEST_INTERVAL = 6  # 60 seconds / 10 requests = 6 seconds per request

def fetch_matches_for_competitions(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """Fetches matches from top 5 leagues for the specified date range."""
    all_matches = []

    for competition in COMPETITION_CODES:
        url = f"{BASE_URL}/competitions/{competition}/matches"
        params = {
            'dateFrom': date_from,
            'dateTo': date_to,
            'status': 'FINISHED'
        }

        try:
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 429:
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params)

            response.raise_for_status()
            data = response.json()
            matches = data.get('matches', [])
            logging.info(f"Fetched {len(matches)} matches for competition {competition}")
            all_matches.extend(matches)

            time.sleep(REQUEST_INTERVAL)

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred for competition {competition}: {e}")
        except Exception as e:
            logging.error(f"An error occurred for competition {competition}: {e}")

    return all_matches

def save_to_gcs(data: dict, match_id: int) -> None:
    """Saves the match data to a GCS bucket as a JSON file if it doesn't already exist."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"match_data/{match_id}.json")
    
    if not blob.exists():
        try:
            blob.upload_from_string(
                data=json.dumps(data),
                content_type='application/json'
            )
            logging.info(f"Saved match ID {match_id} data to GCS.")
        except Exception as e:
            logging.error(f"Error saving match ID {match_id} data to GCS: {e}")
            raise
    else:
        logging.info(f"Match ID {match_id} already exists in GCS, skipping.")

def validate_environment() -> bool:
    """Validates required environment variables."""
    required_vars = [
        'API_FOOTBALL_KEY',
        'GCP_PROJECT_ID',
        'BUCKET_NAME'
    ]
    return all(os.environ.get(var) for var in required_vars)
