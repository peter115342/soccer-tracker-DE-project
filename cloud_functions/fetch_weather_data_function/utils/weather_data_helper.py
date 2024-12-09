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

        data = {
            "latitude": response.Latitude,
            "longitude": response.Longitude,
            "timezone": response.Timezone,
            "timezone_abbreviation": response.TimezoneAbbreviation,
            "hourly": {
                "temperature_2m": response.Hourly.Temperature2m,
                "relativehumidity_2m": response.Hourly.RelativeHumidity2m,
                "dewpoint_2m": response.Hourly.Dewpoint2m,
                "apparent_temperature": response.Hourly.ApparentTemperature,
                "precipitation": response.Hourly.Precipitation,
                "rain": response.Hourly.Rain,
                "snowfall": response.Hourly.Snowfall,
                "snow_depth": response.Hourly.SnowDepth,
                "weathercode": response.Hourly.WeatherCode,
                "pressure_msl": response.Hourly.PressureMsl,
                "cloudcover": response.Hourly.CloudCover,
                "windspeed_10m": response.Hourly.WindSpeed10m,
                "winddirection_10m": response.Hourly.WindDirection10m,
                "windgusts_10m": response.Hourly.WindGusts10m,
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
