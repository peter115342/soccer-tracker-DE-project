import requests
import logging
from typing import Dict, Any
from datetime import datetime, timezone
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

    date_str = match_datetime.strftime("%Y-%m-%d")
    current_datetime = datetime.now(timezone.utc)

    hourly_variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "snow_depth",
        "weather_code",
        "pressure_msl",
        "cloudcover",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ]

    if match_datetime <= current_datetime:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date_str,
            "end_date": date_str,
            "hourly": ",".join(hourly_variables),
            "timezone": "UTC",
        }
        response = requests.get(BASE_URL, params=params)
    else:
        forecast_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ",".join(hourly_variables),
            "timezone": "UTC",
        }
        response = requests.get(forecast_url, params=params)

    try:
        time.sleep(RATE_LIMIT_DELAY)
        response.raise_for_status()
        data = response.json()

        if "hourly" in data and any(data["hourly"].values()):
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
