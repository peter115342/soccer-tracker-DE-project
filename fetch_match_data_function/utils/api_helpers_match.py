import requests
from typing import List, Dict, Any
import os
import time
import logging

API_KEY = os.environ.get('API_FOOTBALL_KEY')
BASE_URL = 'https://api.football-data.org/v4'
HEADERS = {'X-Auth-Token': API_KEY}

REQUESTS_PER_MINUTE = 10
REQUEST_INTERVAL = 60
MAX_RETRIES = 5

COMPETITION_CODES = ['PL', 'BL1', 'PD', 'SA', 'FL1']

def fetch_matches_by_date_range(date_from: str, date_to: str) -> List[Dict[str, Any]]:
    all_matches = []
    
    for competition in COMPETITION_CODES:
        url = f"{BASE_URL}/competitions/{competition}/matches"
        params = {
            'dateFrom': date_from,
            'dateTo': date_to,
            'status': 'FINISHED'
        }
        
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            if response.status_code == 429:
                time.sleep(60)
                response = requests.get(url, headers=HEADERS, params=params)
            
            response.raise_for_status()
            data = response.json()
            matches = data.get('matches', [])
            logging.info(f"Fetched {len(matches)} matches for competition {competition}")
            all_matches.extend(matches)
            
            # Respect rate limiting
            time.sleep(6)  # 10 requests per minute = 1 request per 6 seconds
            
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred for competition {competition}: {e}")
        except Exception as e:
            logging.error(f"An error occurred for competition {competition}: {e}")
    
    return all_matches
