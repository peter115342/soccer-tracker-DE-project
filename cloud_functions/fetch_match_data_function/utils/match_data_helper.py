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
REQUEST_INTERVAL = 6  # 60 seconds / 10 requests = 6 seconds per request

def fetch_matches_for_competitions(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    """Fetches matches from top 5 leagues for the specified date range."""
    all_matches = []
    total_matches_found = 0
    new_matches_count = 0

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    for competition in COMPETITION_CODES:
        logging.info(f"Fetching matches for competition {competition}")
        url = f"{BASE_URL}/competitions/{competition}/matches"
        params = {
            'dateFrom': date_from,
            'dateTo': date_to,
            'status': 'FINISHED'
        }

        try:
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 429:
                logging.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params)

            response.raise_for_status()
            data = response.json()
            matches = data.get('matches', [])
            total_matches_found += len(matches)

            for match in matches:
                match_id = match['id']
                blob = bucket.blob(f"match_data/{match_id}.json")
                if not blob.exists():
                    new_matches_count += 1
                    all_matches.append(match)

            logging.info(f"Competition {competition}: Found {len(matches)} matches, {new_matches_count} are new")
            time.sleep(REQUEST_INTERVAL)

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred for competition {competition}: {e}")
        except Exception as e:
            logging.error(f"An error occurred for competition {competition}: {e}")

    logging.info(f"Total matches found: {total_matches_found}, New matches to process: {new_matches_count}")
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
            logging.info(f"Saved new match ID {match_id} to GCS")
        except Exception as e:
            logging.error(f"Error saving match ID {match_id} to GCS: {e}")
            raise
    else:
        logging.info(f"Match ID {match_id} already exists in GCS, skipping")
