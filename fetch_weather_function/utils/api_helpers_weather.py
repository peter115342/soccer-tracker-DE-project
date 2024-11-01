import requests
import logging
from typing import Dict, Any
from datetime import datetime, timezone

BASE_URL = 'https://archive-api.open-meteo.com/v1/archive'


def fetch_weather_by_coordinates(lat: float, lon: float, match_datetime: datetime) -> Dict[str, Any]:
    date_str = match_datetime.strftime('%Y-%m-%d')
    current_datetime = datetime.now(timezone.utc)

    if match_datetime < current_datetime:
        base_url = 'https://archive-api.open-meteo.com/v1/archive'
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': date_str,
            'end_date': date_str,
            'hourly': ','.join([...]),
            'timezone': 'UTC',
        }
    else:
        # Use Forecast API
        base_url = 'https://api.open-meteo.com/v1/forecast'
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': date_str,
            'end_date': date_str,
            'hourly': ','.join([...]),
            'timezone': 'UTC',
        }

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

