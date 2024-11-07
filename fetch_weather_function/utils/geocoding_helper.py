import requests
import logging
import re
import os
from typing import Dict, Optional, Union

MAPBOX_ACCESS_TOKEN = os.environ.get('MAPBOX_ACCESS_TOKEN')

def extract_postcode(address: str) -> Optional[str]:
    if not address or address.lower() == "null":
        logging.info("Address is null or empty, will try city extraction")
        return None

    postcode_patterns = [
        r'\b[A-Z]{1,2}\d{1,2}[A-Z]? ?\d[A-Z]{2}\b',  # UK Postcode
        r'\b\d{5}\b',                                 # 5-digit ZIP codes
        r'\b\d{5}-\d{4}\b',                           # US ZIP+4 codes
        r'\b\d{3}\s?\d{2}\b',                         # Italian CAP
        r'\b\d{4}\b',                                 # 4-digit codes
    ]

    for pattern in postcode_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            postcode = match.group(0).strip()
            logging.debug(f"Extracted postcode '{postcode}' from address '{address}'")
            return postcode

    logging.info(f"No postcode found in address '{address}', will try city extraction")
    return None

def extract_city(address: str) -> Optional[str]:
    """
    Extracts the city as the second to last word in the address string.
    """
    if not address or address.lower() == "null":
        logging.warning("Address is null or empty, cannot extract city")
        return None

    address_parts = [part for part in re.split(r'[,\s]+', address.strip()) if part.lower() != 'null']

    if len(address_parts) >= 2:
        city = address_parts[-2]
        logging.debug(f"Extracted city '{city}' from address '{address}'")
        return city
    elif address_parts:
        city = address_parts[-1]
        logging.debug(f"Extracted city '{city}' from address '{address}'")
        return city
    else:
        logging.warning(f"Could not extract city from address '{address}'")
        return None

def get_coordinates_from_location(location: str, country_code: Optional[str] = None) -> Optional[Dict[str, float]]:
    """
    Use Mapbox Geocoding API to get latitude and longitude from a location (postcode or city).
    """
    if not MAPBOX_ACCESS_TOKEN:
        logging.error("MAPBOX_ACCESS_TOKEN environment variable not set.")
        return None

    encoded_location = requests.utils.quote(location)
    
    url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{encoded_location}.json'
    params: Dict[str, Union[str, int, bool]] = {
        'access_token': MAPBOX_ACCESS_TOKEN,
        'limit': 1,
        'types': 'place,locality,neighborhood,address,postcode',
        'fuzzyMatch': True
    }
    
    if country_code:
        params['country'] = country_code.lower()
        coords = try_mapbox_request(url, params)
        if coords:
            return coords
            
    params.pop('country', None)
    coords = try_mapbox_request(url, params)
    if coords:
        return coords
            
    osm_url = "https://nominatim.openstreetmap.org/search"
    osm_params: Dict[str, Union[str, int]] = {
        'q': location,
        'format': 'json',
        'limit': 1,
        'addressdetails': 1
    }
    
    headers = {'User-Agent': 'FootballWeatherApp/1.0'}
    try:
        osm_response = requests.get(osm_url, params=osm_params, headers=headers)
        osm_response.raise_for_status()
        osm_data = osm_response.json()
        
        if osm_data:
            lat = float(osm_data[0]['lat'])
            lon = float(osm_data[0]['lon'])
            logging.info(f"Got coordinates from OSM for '{location}': (lat: {lat}, lon: {lon})")
            return {'lat': lat, 'lon': lon}
    except Exception as e:
        logging.error(f"OSM API error for location '{location}': {e}")
    
    return None

def try_mapbox_request(url: str, params: Dict[str, Union[str, int, bool]]) -> Optional[Dict[str, float]]:
    """Helper function to make Mapbox API requests"""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        
        if features:
            coords = features[0]['center']
            lon, lat = coords[0], coords[1]
            logging.info(f"Successfully obtained coordinates: (lat: {lat}, lon: {lon})")
            return {'lat': lat, 'lon': lon}
    except Exception as e:
        logging.error(f"Mapbox API error: {e}")
    return None

def get_coordinates(address: str, country_code: Optional[str] = None) -> Optional[Dict[str, float]]:
    """
    Gets the coordinates for an address by first trying the postcode,
    and if not found, using the city.

    Args:
        address (str): The full address string.
        country_code (Optional[str]): Optional country code to narrow down the search.

    Returns:
        Optional[Dict[str, float]]: Coordinates dictionary if successful, otherwise None.
    """
    postcode = extract_postcode(address)
    if postcode:
        coords = get_coordinates_from_location(postcode, country_code)
        if coords:
            return coords
        else:
            logging.warning(f"Failed to get coordinates using postcode '{postcode}'")
    else:
        logging.info("Postcode not found, attempting to use city.")

    city = extract_city(address)
    if city:
        coords = get_coordinates_from_location(city, country_code)
        if coords:
            return coords
        else:
            logging.error(f"Failed to get coordinates using city '{city}'")
    else:
        logging.error("City not found in address.")

    return None
