import requests
import logging
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
import json
from google.cloud import storage
import os
import time

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")
RATE_LIMIT_DELAY = 0.5


def fetch_weather_by_coordinates(
    lat: float, lon: float, match_datetime: datetime
) -> Dict[str, Any]:
    """Fetches historical or forecast weather data from Open-Meteo API based on coordinates and match datetime."""

    if match_datetime.tzinfo is None:
        match_datetime = match_datetime.replace(tzinfo=timezone.utc)

    start_date = match_datetime.date()
    end_date = (match_datetime + timedelta(days=1)).date()

    hourly_variables = [
        "temperature_2m",
        "relativehumidity_2m",
        "dewpoint_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "weathercode",
        "pressure_msl",
        "cloudcover",
        "windspeed_10m",
        "winddirection_10m",
        "windgusts_10m",
    ]

    current_datetime = datetime.now(timezone.utc)
    yesterday = current_datetime.date() - timedelta(days=1)

    try:
        if match_datetime.date() >= yesterday:
            forecast_url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": ",".join(hourly_variables),
                "timezone": "UTC",
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "past_days": 1,
            }
            response = requests.get(forecast_url, params=params)
        else:
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "hourly": ",".join(hourly_variables),
                "timezone": "UTC",
                "models": "best_match",
            }
            response = requests.get(BASE_URL, params=params)

        time.sleep(RATE_LIMIT_DELAY)
        response.raise_for_status()
        data = response.json()

        if "hourly" in data and any(data["hourly"].values()):
            logging.info(
                f"Successfully fetched weather data for coordinates: {lat}, {lon}"
            )
            return data
        else:
            logging.error(f"Invalid or empty data received: {data}")
            return {}

    except requests.exceptions.HTTPError as e:
        logging.error(
            f"HTTP error occurred while fetching weather data from Open-Meteo: {e}"
        )
        logging.debug(f"Response content: {response.text}")
    except Exception as e:
        logging.error(
            f"An error occurred while fetching weather data from Open-Meteo: {e}"
        )

    return {}


def save_weather_to_gcs(data: dict, match_id: int) -> bool:
    """Saves the weather data to a GCS bucket as a JSON file if it doesn't already exist.
    Returns True if new data was saved, False if data already existed."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"weather_data/{match_id}.json")

    if not blob.exists():
        try:
            blob.upload_from_string(
                data=json.dumps(data), content_type="application/json"
            )
            logging.info(f"Saved weather data for match ID {match_id} to GCS")
            return True
        except Exception as e:
            logging.error(
                f"Error saving weather data for match ID {match_id} to GCS: {e}"
            )
            raise
    else:
        logging.info(
            f"Weather data for match ID {match_id} already exists in GCS, skipping"
        )
        return False
