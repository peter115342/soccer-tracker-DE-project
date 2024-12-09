import logging
from typing import Dict, Any
from datetime import datetime, timezone
import json
from google.cloud import storage
import os
from openmeteo_requests import Client
from retry_requests import retry
import requests

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def fetch_weather_by_coordinates(
    lat: float, lon: float, match_datetime: datetime
) -> Dict[str, Any]:
    """Fetches historical or forecast weather data using openmeteo-requests client."""

    date_str = match_datetime.strftime("%Y-%m-%d")
    current_datetime = datetime.now(timezone.utc)

    cache_session = requests.Session()
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = Client(session=retry_session)

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "timezone": "UTC",
        "hourly": [
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
            "cloud_cover",
            "visibility",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
    }

    try:
        if match_datetime < current_datetime:
            url = "https://archive-api.open-meteo.com/v1/archive"
        else:
            url = "https://api.open-meteo.com/v1/forecast"

        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        hourly = response.Hourly()

        data = {
            "latitude": response.Latitude(),
            "longitude": response.Longitude(),
            "timezone": response.Timezone(),
            "timezone_abbreviation": response.TimezoneAbbreviation(),
            "hourly": {
                "time": hourly.Variables(0).Values(),
                "temperature_2m": hourly.Variables(1).Values(),
                "relativehumidity_2m": hourly.Variables(2).Values(),
                "dewpoint_2m": hourly.Variables(3).Values(),
                "apparent_temperature": hourly.Variables(4).Values(),
                "precipitation": hourly.Variables(5).Values(),
                "rain": hourly.Variables(6).Values(),
                "snowfall": hourly.Variables(7).Values(),
                "snow_depth": hourly.Variables(8).Values(),
                "weathercode": hourly.Variables(9).Values(),
                "pressure_msl": hourly.Variables(10).Values(),
                "cloudcover": hourly.Variables(11).Values(),
                "visibility": hourly.Variables(12).Values(),
                "windspeed_10m": hourly.Variables(13).Values(),
                "winddirection_10m": hourly.Variables(14).Values(),
                "windgusts_10m": hourly.Variables(15).Values(),
            },
        }

        logging.info(
            f"Successfully fetched weather data for coordinates ({lat}, {lon}) on {date_str}"
        )
        logging.debug(f"Weather data: {json.dumps(data, indent=2)}")

        return data

    except Exception as e:
        logging.error(f"Error fetching weather data: {str(e)}")
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
