from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging

def process_match_data(match_data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    processed_matches = {}
    logging.info(f"Processing {len(match_data_list)} matches")
    for match in match_data_list:
        match_id = match.get('id')
        if not match_id:
            logging.warning("Missing 'id' in match data. Assigning a temporary ID.")
            match_id = f"temp_{hash(frozenset(match.items()))}"

        if match_id not in processed_matches:
            home_team = match.get('homeTeam', {})
            away_team = match.get('awayTeam', {})
            competition = match.get('competition', {})
            score = match.get('score', {})

            utc_date_str:  Optional[str] = match.get('utcDate')
            if not isinstance(utc_date_str, str):
                continue

            utc_date = datetime.strptime(utc_date_str, '%Y-%m-%dT%H:%M:%SZ')
            cet_date = utc_date + timedelta(hours=1)  # UTC+1 for CET

            processed_match = {
                'id': match_id,
                'utcDate': utc_date_str,
                'cetDate': cet_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'status': match.get('status'),
                'matchday': match.get('matchday'),
                'stage': match.get('stage'),
                'lastUpdated': match.get('lastUpdated'),
                'homeTeam': {
                    'id': home_team.get('id'),
                    'name': home_team.get('name')
                },
                'awayTeam': {
                    'id': away_team.get('id'),
                    'name': away_team.get('name')
                },
                'competition': {
                    'id': competition.get('id'),
                    'name': competition.get('name')
                },
                'score': {
                    'winner': score.get('winner'),
                    'duration': score.get('duration'),
                    'fullTime': {
                        'homeTeam': score.get('fullTime', {}).get('home'),
                        'awayTeam': score.get('fullTime', {}).get('away')
                    },
                    'halfTime': {
                        'homeTeam': score.get('halfTime', {}).get('home'),
                        'awayTeam': score.get('halfTime', {}).get('away')
                    }
                }
            }
            processed_matches[match_id] = processed_match

    return list(processed_matches.values())
