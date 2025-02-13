import os
import json
import logging
import time
from typing import List, Dict, Any
from datetime import datetime, timedelta
import requests
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_FOOTBALL_KEY")
BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")

COMPETITION_MAPPING = {
    "2021": "PL",  # Premier League
    "2014": "PD",  # La Liga
    "2019": "SA",  # Serie A
    "2015": "FL1",  # Ligue 1
    "2002": "BL1",  # Bundesliga
}
REQUEST_INTERVAL = 6  # Seconds between API requests


def get_unique_dates() -> List[str]:
    """
    Fetch only unprocessed unique dates from matches_processed table.
    Returns only dates up to current date in descending order.
    """
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT DATE(utcDate) as match_date
        FROM `{GCP_PROJECT_ID}.sports_data_eu.matches_processed`
        WHERE DATE(utcDate) <= '2024-12-01'
        ORDER BY match_date DESC
    """  # nosec B608
    query_job = client.query(query)
    return [row.match_date.strftime("%Y-%m-%d") for row in query_job]


def get_processed_standings_dates() -> List[str]:
    """
    Get dates that already have standings data in GCS.
    Checks existing files in the standings_data/ prefix.
    """
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="standings_data/")

    processed_dates = set()
    for blob in blobs:
        date = blob.name.split("/")[1].split("_")[0]
        processed_dates.add(date)

    return list(processed_dates)


def should_fetch_standings(date: str, processed_dates: List[str]) -> bool:
    """
    Determine if standings should be fetched for a given date.
    Returns True for today's date or unprocessed past dates.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    if date == today:
        return True
    elif date > today:
        return False
    else:
        return date not in processed_dates


def fetch_standings_for_date(date: str) -> List[Dict[str, Any]]:
    """
    Fetches standings for all competitions on a specific date.
    Uses the next day's date for the API call to ensure complete data.
    Handles rate limiting and includes error handling for each request.
    """
    all_standings = []

    next_date = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )

    for comp_id, comp_code in COMPETITION_MAPPING.items():
        logging.info(
            f"Fetching standings for competition {comp_id} ({comp_code}) on {date}"
        )
        url = f"{BASE_URL}/competitions/{comp_code}/standings"
        params = {"date": next_date}

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=90)
            if response.status_code == 429:
                logging.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params, timeout=90)

            response.raise_for_status()
            standings_data = response.json()

            if standings_data.get("season", {}).get("winner"):
                standings_data["season"]["winner"] = standings_data["season"]["winner"][
                    "id"
                ]

            standings_data["fetchDate"] = date
            standings_data["competitionId"] = comp_id
            all_standings.append(standings_data)

            time.sleep(REQUEST_INTERVAL)

        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred for competition {comp_code}: {e}")
        except Exception as e:
            logging.error(f"An error occurred for competition {comp_code}: {e}")

    return all_standings


def save_standings_to_gcs(
    standings_data: dict, date: str, competition_code: str
) -> None:
    """
    Saves the standings data to GCS using date_competitioncode.json format.
    Includes error handling and logging for storage operations.
    """
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
        error_msg = (
            f"Error saving standings for {competition_code} on {date} to GCS: {e}"
        )
        logging.error(error_msg)
        raise Exception(error_msg)
