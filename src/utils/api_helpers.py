import requests
import os

API_KEY = os.getenv('API_FOOTBALL_KEY')
BASE_URL = 'https://v3.football.api-sports.io/'
HEADERS = {'x-apisports-key': API_KEY}
TOP_LEAGUE_IDS = [39, 140, 78, 135, 61]

def fetch_fixtures_by_date(date):
    fixtures = []
    for league_id in TOP_LEAGUE_IDS:
        url = f"{BASE_URL}fixtures?date={date}&league={league_id}"
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            fixtures.extend(response.json()['response'])
        else:
            print(f"Error fetching fixtures for league {league_id}: {response.status_code}")
    return fixtures

def fetch_match_statistics(fixture_id):
    url = f"{BASE_URL}fixtures/statistics?fixture={fixture_id}"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 200:
        return response.json()['response']
    else:
        print(f"Error fetching statistics for fixture {fixture_id}: {response.status_code}")
        return []