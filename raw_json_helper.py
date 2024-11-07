import os
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests_ratelimiter import LimiterSession
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get('API_FOOTBALL_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET')
BIGQUERY_TABLE = os.environ.get('BIGQUERY_TABLE', 'match_data')
GCS_BUCKET_NAME = os.environ.get('BUCKET_NAME')

REQUESTS_PER_MINUTE = 5
BACKOFF_TIME = 60

session = LimiterSession(
    per_minute=REQUESTS_PER_MINUTE,
    burst_count=1,
    realm="football-api",
    timeout=30
)
session.headers.update(HEADERS)

retries = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
    respect_retry_after_header=True
)
adapter = HTTPAdapter(max_retries=retries)
session.mount('https://', adapter)

def fetch_ids() -> List[int]:
    """Fetches unique match IDs from the BigQuery match_data table."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT DISTINCT id
        FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    """
    query_job = client.query(query)
    results = query_job.result()
    
    ids = [row.id for row in results]
    logging.info(f"Fetched {len(ids)} match IDs from BigQuery.")
    return ids

def fetch_match_data(id: int) -> Optional[dict]:
    """Fetches match data from the football-data.org API for a given match ID."""
    url = f"{BASE_URL}/matches/{id}"
    try:
        response = session.get(url)
        response.raise_for_status()
        logging.info(f"Fetched data for match ID: {id}")
        return response.json()
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error for match ID {id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching data for match ID {id}: {e}")
    return None

def save_to_gcs(data: dict, id: int) -> None:
    """Saves the match data to a GCS bucket as a JSON file."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"match_data/{id}.json")
    try:
        blob.upload_from_string(
            data=json.dumps(data),
            content_type='application/json'
        )
        logging.info(f"Saved match ID {id} data to GCS.")
    except Exception as e:
        logging.error(f"Error saving match ID {id} data to GCS: {e}")

def process_batch(match_ids: List[int]) -> dict:
    """Process a batch of match IDs and return statistics."""
    success_count = 0
    error_count = 0
    
    for match_id in match_ids:
        try:
            data = fetch_match_data(match_id)
            if data:
                save_to_gcs(data, match_id)
                success_count += 1
        except Exception as e:
            error_count += 1
            logging.error(f"Failed to process match ID {match_id}: {e}")
    
    return {
        'success_count': success_count,
        'error_count': error_count
    }

def validate_environment() -> bool:
    """Validates required environment variables."""
    required_vars = [
        'API_FOOTBALL_KEY',
        'GCP_PROJECT_ID',
        'BIGQUERY_DATASET',
        'BUCKET_NAME'
    ]
    return all(os.environ.get(var) for var in required_vars)
