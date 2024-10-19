from typing import List, Dict, Any


def process_match_data(match_data_list: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
    processed_matches = []
    for match in match_data_list:
        processed_match = {
            'id': match['id'],
            'utcDate': match['utcDate'],
            'status': match['status'],
            'matchday': match.get('matchday'),
            'stage': match.get('stage'),
            'lastUpdated': match.get('lastUpdated'),
            'homeTeam': {
                'id': match['homeTeam']['id'],
                'name': match['homeTeam']['name']
            },
            'awayTeam': {
                'id': match['awayTeam']['id'],
                'name': match['awayTeam']['name']
            },
            'score': match['score'],
            'venue': match.get('venue'),
            'attendance': match.get('attendance'),
            'competition': {
                'id': match['competition']['id'],
                'name': match['competition']['name'],
                'area': match['competition']['area']['name'],
            },
        }
        processed_matches.append(processed_match)
    return processed_matches
