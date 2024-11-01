import requests
import logging
import re
import os
from typing import Dict, Optional

MAPBOX_ACCESS_TOKEN = os.environ.get('MAPBOX_ACCESS_TOKEN')

def extract_postcode(address: str) -> Optional[str]:
    """
    Extracts the postcode from an address string.
    Supports multiple countries' postcodes.

    Args:
        address (str): The address string to extract the postcode from.

    Returns:
        Optional[str]: The extracted postcode if found, otherwise None.
    """
    postcode_patterns = [
        r'\b[A-Z]{1,2}\d{1,2}[A-Z]? ?\d[A-Z]{2}\b',  # UK Postcode
        r'\b\d{5}\b',                                 # 5-digit ZIP codes (Germany, France, Spain, Italy, etc.)
        r'\b\d{5}-\d{4}\b',                           # US ZIP+4 codes
        r'\b\d{3}\s?\d{2}\b',                         # Italian CAP
        r'\b\d{4}\b',                                 # 4-digit codes (e.g., Austria, Switzerland)
    ]

    for pattern in postcode_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            postcode = match.group(0).strip()
            logging.debug(f"Extracted postcode '{postcode}' from address '{address}'")
            return postcode

    logging.warning(f"No postcode found in address '{address}'")
    return None

def get_coordinates_from_postcode(postcode: str, country_code: Optional[str] = None) -> Optional[Dict[str, float]]:
    """
    Use Mapbox Geocoding API to get latitude and longitude from a postcode.

    Args:
        postcode (str): The postcode to geocode.
        country_code (Optional[str]): Optional country code (ISO 3166-1 alpha-2) to narrow down the search.

    Returns:
        Optional[Dict[str, float]]: A dictionary with 'lat' and 'lon' keys if successful, otherwise None.
    """
    if not MAPBOX_ACCESS_TOKEN:
        logging.error("MAPBOX_ACCESS_TOKEN environment variable not set.")
        return None

    url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{postcode}.json'

    params = {
        'access_token': MAPBOX_ACCESS_TOKEN,
        'limit': 1,
        'types': 'postcode',
    }
    if country_code:
        params['country'] = country_code.lower()

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        if features:
            coords = features[0]['center']  # [longitude, latitude]
            lon, lat = coords[0], coords[1]
            logging.info(f"Obtained coordinates for postcode '{postcode}': (lat: {lat}, lon: {lon})")
            return {'lat': lat, 'lon': lon}
        else:
            logging.error(f"No coordinates found for postcode '{postcode}'")
    except Exception as e:
        logging.error(f"Error fetching coordinates for postcode '{postcode}': {e}")
    return None
