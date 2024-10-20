
import os
from utils.api_helpers import get_league_data
from utils.bigquery_helpers import insert_data_into_bigquery
from typing import Dict, Any, List

def fetch_league_data(request):
    """
    Cloud Function to fetch leagues and teams data and load into BigQuery.
    """
    league_codes = ['PL', 'FL1', 'BL1', 'SA', 'PD']  # Premier League, Ligue 1, Bundesliga, Serie A, La Liga
    leagues: List[Dict[str, Any]] = []
    teams: List[Dict[str, Any]] = []

    for code in league_codes:
        league_data = get_league_data(code)

        league_info = {
            'id': league_data['id'],
            'name': league_data['name'],
            'code': league_data['code'],
            'logo': league_data.get('emblem'),
            'current_season_start_date': league_data['currentSeason']['startDate'],
            'current_season_end_date': league_data['currentSeason']['endDate'],
            'current_season_matchday': league_data['currentSeason'].get('currentMatchday'),
        }
        leagues.append(league_info)

        for team in league_data['teams']:
            team_info = {
                'id': team['id'],
                'name': team['name'],
                'tla': team.get('tla'),
                'logo': team.get('crest'),
                'venue': team.get('venue'),
                'league_id': league_data['id'],
            }
            teams.append(team_info)

    insert_data_into_bigquery('leagues', leagues)
    insert_data_into_bigquery('teams', teams)

    return 'Data successfully inserted into BigQuery.', 200
