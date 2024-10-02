import requests
import os
import time

API_KEY = os.getenv('API_FOOTBALL_KEY')
BASE_URL = 'https://v3.football.api-sports.io/'
HEADERS = {'x-apisports-key': API_KEY}
LEAGUE_IDS = [39, 140, 78, 135, 61, 88, 94]

def fetch_all_fixtures_by_date(date):
    url = f"{BASE_URL}fixtures?date={date}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()['response']
    else:
        print(f"Error fetching fixtures: {response.status_code}")
        return []

def filter_fixtures_by_leagues(fixtures, league_ids):
    return [fixture for fixture in fixtures if fixture['league']['id'] in league_ids]


def fetch_match_statistics(fixture_id, index, total_fixtures):
    if index > 0:
        print(f"Waiting for 6 seconds to respect API rate limit...")
        time.sleep(6)

    try:
        url = f"{BASE_URL}fixtures/statistics?fixture={fixture_id}"
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        print(f"Fetched statistics for fixture {fixture_id} ({index + 1}/{total_fixtures}).")
        return response.json()['response']
    except requests.exceptions.RequestException as e:
        print(f"Request exception for fixture {fixture_id}: {e}")
        return []