from typing import List, Dict, Any
from flask import Request
from datetime import datetime

from utils.api_helpers_league import get_league_data
from utils.bigquery_helpers_league import insert_data_into_bigquery
import requests
import json
import os
import logging

def is_odd_week():
    return datetime.now().isocalendar()[1] % 2 == 1

def fetch_league_data(request: Request):
    """
    Cloud Function to fetch leagues and teams data and load into BigQuery,
    runs only on odd weeks with Discord notifications.
    """
    if not is_odd_week():
        send_discord_notification(
            "⏭️ Fetch League Data: Skipped", 
            "Run skipped - scheduled for odd weeks only.",
            16776960
        )
        return 'Run skipped - scheduled for odd weeks only.', 200

    try:
        league_codes = ['PL', 'FL1', 'BL1', 'SA', 'PD']  # Premier League, Ligue 1, Bundesliga, Serie A, La Liga
        league_data_list = []

        for code in league_codes:
            league_data = get_league_data(code)
            league_data_list.append(league_data)

        load_data_into_bigquery(league_data_list)
        send_discord_notification(
            "✅ Fetch League Data: Success", 
            "League data fetched and loaded into BigQuery successfully.",
            65280  # Green
        )
        return 'League data fetched and loaded into BigQuery successfully.', 200
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch League Data: Failure", 
            error_message,
            16711680  # Red
        )
        raise

def load_data_into_bigquery(league_data_list: List[Dict[str, Any]]):
    league_table_name = 'leagues'
    team_table_name = 'teams'

    leagues = []
    teams = []

    for league_data in league_data_list:
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
                'address': team.get('address'),
                'league_id': league_data['id'],
            }
            teams.append(team_info)

    insert_data_into_bigquery(league_table_name, leagues)
    insert_data_into_bigquery(team_table_name, teams)

def send_discord_notification(title: str, message: str, color: int):
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return
    discord_data = {
        "content": None,
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": color,
                "footer": {
                    "text": "Football Data Processing Service"
                }
            }
        ]
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        logging.error(f"Failed to send Discord notification: {response.status_code}, {response.text}")
