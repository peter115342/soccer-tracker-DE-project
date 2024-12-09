import requests
import logging
from typing import Dict, Any, Union
from datetime import datetime, timezone
import json
from google.cloud import storage
import os

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")


def fetch_weather_by_coordinates(
    lat: float, lon: float, match_datetime: datetime
) -> Dict[str, Any]:
    """Fetches historical or forecast weather data from Open-Meteo API based on coordinates and match datetime."""

    date_str = match_datetime.strftime("%Y-%m-%d")
    current_datetime = datetime.now(timezone.utc)

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

    if match_datetime < current_datetime:
        base_url = "https://archive-api.open-meteo.com/v1/archive"
        params: Dict[str, Union[str, float]] = {
            "latitude": f"{lat:.7f}",
            "longitude": f"{lon:.7f}",
            "start_date": date_str,
            "end_date": date_str,
            "hourly": ",".join(hourly_variables),
            "timezone": "UTC",
        }
    else:
        base_url = "https://api.open-meteo.com/v1/forecast"
        params: Dict[str, Union[str, float]] = {
            "latitude": f"{lat:.7f}",
            "longitude": f"{lon:.7f}",
            "start_date": date_str,
            "end_date": date_str,
            "hourly": ",".join(hourly_variables),
            "timezone": "UTC",
        }

    try:
        logging.debug(f"Making API request to: {base_url}")
        logging.debug(f"Parameters: {json.dumps(params, indent=2)}")

        response = requests.get(base_url, params=params)
        logging.debug(f"API Request URL: {response.url}")
        response.raise_for_status()
        data = response.json()
        logging.info(
            f"Fetched weather data from Open-Meteo for coordinates ({lat}, {lon}) on {date_str}"
        )

        logging.debug(f"Open-Meteo API response: {json.dumps(data, indent=2)}")

        return data
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
