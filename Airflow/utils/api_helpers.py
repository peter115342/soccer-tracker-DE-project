import requests
from typing import List, Dict, Any
import os
import time

API_KEY = os.getenv('FOOTBALL_DATA_API_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}
REQUEST_LIMIT = 10
REQUEST_INTERVAL = 60 

COMPETITION_IDS = [2002, 2014, 2015, 2019, 2021]  # BL1, PD, FL1, SA, PL

def fetch_fixtures_by_date(date_str: str) -> List[Dict[str, Any]]:
    competitions = ','.join(str(comp_id) for comp_id in COMPETITION_IDS)
    url = f"{BASE_URL}/matches?competitions={competitions}&dateFrom={date_str}&dateTo={date_str}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    fixtures = data.get('matches', [])
    return fixtures

def fetch_match_statistics(match_ids: List[int]) -> List[Dict[str, Any]]:
    all_stats = []
    request_count = 0
    for index, match_id in enumerate(match_ids):
        if request_count >= REQUEST_LIMIT:
            print("API request limit reached. Sleeping for 60 seconds.")
            time.sleep(REQUEST_INTERVAL)
            request_count = 0
        url = f"{BASE_URL}/matches/{match_id}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        match_data = response.json().get('match', {})
        all_stats.append(match_data)
        request_count += 1
    return all_stats

def get_league_data(league_code: str) -> Dict[str, Any]:
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
