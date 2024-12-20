import os
import json
import logging
import time
from typing import List, Dict, Any
from datetime import datetime
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
    """Fetch unique utcDates from matches_processed table."""
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT DATE(utcDate) as match_date
        FROM `{GCP_PROJECT_ID}.sports_data_eu.matches_processed`
        ORDER BY match_date DESC
    """

    query_job = client.query(query)
    return [row.match_date.strftime("%Y-%m-%d") for row in query_job]


def get_processed_standings_dates() -> List[str]:
    """Get dates that already have standings data in GCS."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blobs = bucket.list_blobs(prefix="standings_data/")

    processed_dates = set()
    for blob in blobs:
        date = blob.name.split("/")[1].split("_")[0]
        processed_dates.add(date)

    return list(processed_dates)


def get_competitions_for_date(date: str) -> List[str]:
    """Get competitions that had matches on a specific date."""
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT competition.id as competition_id
        FROM `{GCP_PROJECT_ID}.sports_data_eu.matches_processed`
        WHERE DATE(utcDate) = '{date}'
        AND competition.id IN (2021, 2014, 2019, 2015, 2002)
    """

    query_job = client.query(query)
    return [str(row.competition_id) for row in query_job]


def should_fetch_standings(date: str, processed_dates: List[str]) -> bool:
    """Determine if standings should be fetched for a given date."""
    today = datetime.now().strftime("%Y-%m-%d")

    if date == today:
        return True
    elif date > today:
        return False
    else:
        return date not in processed_dates


def fetch_standings_for_date(date: str) -> List[Dict[str, Any]]:
    """Fetches standings only for competitions that had matches on the specific date."""
    all_standings = []
    competitions = get_competitions_for_date(date)

    for competition in competitions:
        logging.info(f"Fetching standings for competition {competition} on {date}")
        url = f"{BASE_URL}/competitions/{competition}/standings"
        params = {"date": date}

        try:
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 429:
                logging.warning("Rate limit hit, waiting 60 seconds...")
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params)

            response.raise_for_status()
            standings_data = response.json()

            standings_data["fetchDate"] = date
            standings_data["competitionId"] = competition
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
    """Saves the standings data to GCS using date_competitioncode.json format."""
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
