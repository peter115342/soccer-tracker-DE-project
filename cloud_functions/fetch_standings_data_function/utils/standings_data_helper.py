import os
import json
import logging
import time
from typing import List, Dict, Any
import requests
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_FOOTBALL_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")

COMPETITION_CODES = ["PL", "BL1", "PD", "SA", "FL1"]
REQUEST_INTERVAL = 6


def get_unique_dates() -> List[str]:
    """Fetch unique utcDates from matches_processed table"""
    client = bigquery.Client()
    query = """
        SELECT DISTINCT DATE(utcDate) as match_date
        FROM `{}.sports_data_eu.matches_processed`
        ORDER BY match_date DESC
    """.format(GCP_PROJECT_ID)

    query_job = client.query(query)
    return [row.match_date.strftime("%Y-%m-%d") for row in query_job]


def fetch_standings_for_date(date: str) -> List[Dict[str, Any]]:
    """Fetches standings for all competitions for a specific date"""
    all_standings = []

    for competition in COMPETITION_CODES:
        logging.info(f"Fetching standings for competition {competition} on {date}")
        url = f"{BASE_URL}/competitions/{competition}/standings"
        params = {"season": date[:4]}

        try:
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 429:
                logging.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params)

            response.raise_for_status()
            standings_data = response.json()

            standings_data["fetchDate"] = date
            standings_data["competitionCode"] = competition
            all_standings.append(standings_data)

            time.sleep(REQUEST_INTERVAL)

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred for competition {competition}: {e}")
        except Exception as e:
            logging.error(f"An error occurred for competition {competition}: {e}")

    return all_standings


def save_standings_to_gcs(
    standings_data: dict, date: str, competition_code: str
) -> None:
    """Saves the standings data to GCS using date_competitioncode.json format"""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    filename = f"standings_data/{date}_{competition_code}.json"
    blob = bucket.blob(filename)

    try:
        blob.upload_from_string(
            data=json.dumps(standings_data), content_type="application/json"
        )
        logging.info(f"Saved standings for {competition_code} on {date} to GCS")
    except Exception as e:
        logging.error(
            f"Error saving standings for {competition_code} on {date} to GCS: {e}"
        )
        raise
