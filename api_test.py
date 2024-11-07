import requests
import json

uri = 'https://api.football-data.org/v4/competitions/PD/matches?dateFrom=2024-10-26&dateTo=2024-10-26&status=FINISHED'
headers = { 'X-Auth-Token': '964393c13f714528beaa5487d2906fcb' }

response = requests.get(uri, headers=headers)

if response.status_code == 200:
    matches = response.json().get('matches', [])
    match_count = len(matches)
    print(f'Total number of finished matches on 2024-10-26: {match_count}')
    
    for match in matches:
        print(json.dumps(match, indent=2))
else:
    print(f"Error: {response.status_code} - {response.reason}")
    print(response.text)
