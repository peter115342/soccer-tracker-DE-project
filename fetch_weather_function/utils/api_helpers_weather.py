import requests
import logging
from typing import Dict, Any, Union
from datetime import datetime, timezone

BASE_URL = 'https://archive-api.open-meteo.com/v1/archive'

def fetch_weather_by_coordinates(lat: float, lon: float, match_datetime: datetime) -> Dict[str, Any]:
    """Fetches historical or forecast weather data from Open-Meteo API based on coordinates and match datetime."""

    date_str = match_datetime.strftime('%Y-%m-%d')
    current_datetime = datetime.now(timezone.utc)

    if match_datetime < current_datetime:
        base_url = 'https://archive-api.open-meteo.com/v1/archive'
        archive_params: Dict[str, Union[str, float]] = {
            'latitude': lat,
            'longitude': lon,
            'start_date': date_str,
            'end_date': date_str,
            'hourly': ','.join([
                'temperature_2m',
                'relativehumidity_2m',
                'dewpoint_2m',
                'apparent_temperature',
                'precipitation',
                'rain',
                'snowfall',
                'snow_depth',
                'weathercode',
                'pressure_msl',
                'cloudcover',
                'visibility',
                'windspeed_10m',
                'winddirection_10m',
                'windgusts_10m'
            ]),
            'timezone': 'UTC',
        }
        params = archive_params
    else:
        base_url = 'https://api.open-meteo.com/v1/forecast'
        forecast_params: Dict[str, Union[str, float]] = {
            'latitude': lat,
            'longitude': lon,
            'start_date': date_str,
            'end_date': date_str,
            'hourly': ','.join([
                'temperature_2m',
                'relativehumidity_2m',
                'dewpoint_2m',
                'apparent_temperature',
                'precipitation',
                'rain',
                'snowfall',
                'snow_depth',
                'weathercode',
                'pressure_msl',
                'cloudcover',
                'visibility',
                'windspeed_10m',
                'winddirection_10m',
                'windgusts_10m'
            ]),
            'timezone': 'UTC',
        }
        params = forecast_params

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        logging.info(f"Fetched weather data from Open-Meteo for coordinates ({lat}, {lon}) on {date_str}")
        return data
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error occurred while fetching weather data from Open-Meteo: {e}")
    except Exception as e:
        logging.error(f"An error occurred while fetching weather data from Open-Meteo: {e}")

    return {}
