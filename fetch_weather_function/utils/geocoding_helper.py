import requests
import logging
import re
import os
from typing import Dict, Optional, Union

MAPBOX_ACCESS_TOKEN = os.environ.get('MAPBOX_ACCESS_TOKEN')
HERE_API_KEY = os.environ.get('HERE_API_KEY')

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

def get_coordinates_from_here_api(address: str, country_code: str) -> Optional[Dict[str, float]]:
    if not HERE_API_KEY:
        logging.error("HERE_API_KEY environment variable not set.")
        return None
        
    url = "https://geocode.search.hereapi.com/v1/geocode"
    params = {
        'q': address,
        'apiKey': HERE_API_KEY,
        'in': f'countryCode:{country_code}'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data['items']:
            position = data['items'][0]['position']
            logging.info(f"Got coordinates from HERE API for '{address}': (lat: {position['lat']}, lon: {position['lng']})")
            return {'lat': position['lat'], 'lon': position['lng']}
    except Exception as e:
        logging.error(f"HERE API error: {e}")
    return None

def get_coordinates_from_osm(address: str, country_code: str) -> Optional[Dict[str, float]]:
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': f"{address} stadium",
        'format': 'json',
        'countrycodes': country_code,
        'featuretype': 'stadium',
        'limit': 1,
        'addressdetails': 1
    }
    
    headers = {
        'User-Agent': 'FootballWeatherApp/1.0'
    }
    
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data:
            lat = float(data[0]['lat'])
            lon = float(data[0]['lon'])
            logging.info(f"Got coordinates from OSM for '{address}': (lat: {lat}, lon: {lon})")
            return {'lat': lat, 'lon': lon}
    except Exception as e:
        logging.error(f"OSM API error: {e}")
    return None

def try_mapbox_request(url: str, params: Dict[str, Union[str, int, bool]]) -> Optional[Dict[str, float]]:
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        features = data.get('features', [])
        
        if features:
            coords = features[0]['center']
            lon, lat = coords[0], coords[1]
            logging.info(f"Successfully obtained coordinates from Mapbox: (lat: {lat}, lon: {lon})")
            return {'lat': lat, 'lon': lon}
    except Exception as e:
        logging.error(f"Mapbox API error: {e}")
    return None

def get_coordinates_from_location(location: str, country_code: Optional[str] = None) -> Optional[Dict[str, float]]:
    if not MAPBOX_ACCESS_TOKEN:
        logging.error("MAPBOX_ACCESS_TOKEN environment variable not set.")
        return None

    encoded_location = requests.utils.quote(location)
    
    url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{encoded_location}.json'
    params: Dict[str, Union[str, int, bool]] = {
        'access_token': MAPBOX_ACCESS_TOKEN,
        'limit': 1,
        'types': 'address,place,locality,neighborhood,postcode',
        'fuzzyMatch': True
    }
    
    if country_code:
        params['country'] = country_code.lower()
        coords = try_mapbox_request(url, params)
        if coords:
            return coords
            
    params.pop('country', None)
    return try_mapbox_request(url, params)

def get_coordinates(address: str, country_code: Optional[str] = None) -> Optional[Dict[str, float]]:
    if not address or address.lower() == "null":
        logging.warning("Address is null or empty")
        return None

    if country_code:
        coords = get_coordinates_from_here_api(address, country_code)
        if coords:
            return coords
        
        coords = get_coordinates_from_osm(address, country_code)
        if coords:
            return coords

    coords = get_coordinates_from_location(address, country_code)
    if coords:
        return coords

    logging.error(f"Failed to get coordinates for address '{address}' using all available services")
    return None
