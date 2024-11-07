import requests
from typing import Dict, Any
import os

"""Configures football API client with authentication settings."""
API_KEY = os.environ.get('API_FOOTBALL_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}
REQUEST_LIMIT = 10
REQUEST_INTERVAL = 60

COMPETITION_IDS = [2002, 2014, 2015, 2019, 2021]  # BL1, PD, FL1, SA, PL

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
    
    league_data['teams'] = teams_data.get('teams', [])
    
    return league_data
