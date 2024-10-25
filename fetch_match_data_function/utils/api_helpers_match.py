import requests
from typing import List, Dict, Any
import os
import time

API_KEY = os.environ.get('API_FOOTBALL_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}
REQUEST_LIMIT = 10
REQUEST_INTERVAL = 60

COMPETITION_CODES = ['BL1', 'PD', 'FL1', 'SA', 'PL']  # BL1: Bundesliga, PD: La Liga, FL1: Ligue 1, SA: Serie A, PL: Premier League

def fetch_fixtures_by_date(date_str: str) -> List[Dict[str, Any]]:
    competitions = ','.join(COMPETITION_CODES)
    url = f"{BASE_URL}/matches?competitions={competitions}&dateFrom={date_str}&dateTo={date_str}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    fixtures = data.get('matches', [])
    return fixtures

def fetch_match_data(match_ids: List[int]) -> List[Dict[str, Any]]:
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
