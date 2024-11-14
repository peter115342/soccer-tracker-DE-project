import os
import requests
import logging
from typing import Dict, Any, Optional

API_KEY = os.environ.get('API_FOOTBALL_KEY')
GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}

def get_stadium_coordinates(team_name: str) -> Optional[str]:
    """Get stadium coordinates using Google Maps Geocoding API"""
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    search_query = f"{team_name} football stadium"
    
    params = {
        'address': search_query,
        'key': GOOGLE_MAPS_API_KEY
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data['results']:
            location = data['results'][0]['geometry']['location']
            logging.info(f"Successfully retrieved coordinates for {team_name}")
            return f"{location['lat']},{location['lng']}"
    except Exception as e:
        logging.error(f"Error getting coordinates for {team_name}: {e}")
    return None

def get_league_data(league_code: str) -> Dict[str, Any]:
    """Fetches league information and its teams from the football API."""
    if not API_KEY:
        raise ValueError('FOOTBALL_DATA_API_KEY is not set in environment variables.')
    
    league_url = f'{BASE_URL}/competitions/{league_code}'
    league_response = requests.get(league_url, headers=HEADERS)
    league_response.raise_for_status()
    league_data = league_response.json()
    
    teams_url = f'{BASE_URL}/competitions/{league_code}/teams'
    teams_response = requests.get(teams_url, headers=HEADERS)
    teams_response.raise_for_status()
    teams_data = teams_response.json()
    
    for team in teams_data.get('teams', []):
        coordinates = get_stadium_coordinates(team['name'])
        team['address'] = coordinates
        if coordinates:
            logging.info(f"Added coordinates for team: {team['name']}")
        else:
            logging.warning(f"Could not get coordinates for team: {team['name']}")
    
    league_data['teams'] = teams_data.get('teams', [])
    logging.info(f"Successfully fetched data for league code: {league_code}")
    
    return league_data
