import requests
import os
import time
from typing import List, Dict, Any

API_KEY: str = os.getenv('API_FOOTBALL_KEY', '')
BASE_URL: str = 'https://v3.football.api-sports.io/'
HEADERS: Dict[str, str] = {'x-apisports-key': API_KEY}
LEAGUE_IDS: List[int] = [39, 140, 78, 135, 61, 88, 94]

def fetch_all_fixtures_by_date(date: str) -> List[Dict[str, Any]]:
    url: str = f"{BASE_URL}fixtures?date={date}"
    response: requests.Response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()['response']
    else:
        print(f"Error fetching fixtures: {response.status_code}")
        return []

def filter_fixtures_by_leagues(fixtures: List[Dict[str, Any]], league_ids: List[int]) -> List[Dict[str, Any]]:
    return [fixture for fixture in fixtures if fixture['league']['id'] in league_ids]

def fetch_match_statistics(fixture_id: int, index: int, total_fixtures: int) -> List[Dict[str, Any]]:
    if index > 0:
        time.sleep(6)

    try:
        url: str = f"{BASE_URL}fixtures/statistics?fixture={fixture_id}"
        response: requests.Response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        print(f"Fetched statistics for fixture {fixture_id} ({index + 1}/{total_fixtures}).")
        return response.json()['response']
    except requests.exceptions.RequestException as e:
        print(f"Request exception for fixture {fixture_id}: {e}")
        return []
