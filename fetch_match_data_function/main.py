from typing import List, Dict, Any
from flask import Request
from datetime import datetime
import requests
import json
import os

from utils.api_helpers import fetch_fixtures_by_date, fetch_match_data
from utils.data_processing import process_match_data
from utils.bigquery_helpers import insert_data_into_bigquery

def fetch_football_data(request: Request):
    """
    Cloud Function to fetch football match data and load into BigQuery,
    with Discord notifications for success or failure.
    """
    try:
        date_str = get_date_str(request)
        fixtures = fetch_fixtures(date_str)
        
        if not fixtures:
            message = f"No fixtures found for date {date_str}."
            send_discord_notification("⚠️ Fetch Football Data: No Fixtures", message)
            return message, 200

        match_data_list = fetch_matches(fixtures)
        processed_matches = process_matches(match_data_list, date_str)
        result = insert_matches(processed_matches)
        
        success_message = f"Inserted data for {len(processed_matches)} matches into BigQuery."
        send_discord_notification("✅ Fetch Football Data: Success", success_message)
        return result, 200
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification("❌ Fetch Football Data: Failure", error_message)
        raise

def get_date_str(request: Request) -> str:
    request_args = request.args
    if request_args and 'date' in request_args:
        date_str = request_args['date']
    else:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
    return date_str

def fetch_fixtures(date_str: str) -> List[Dict[str, Any]]:
    fixtures = fetch_fixtures_by_date(date_str)
    return fixtures

def fetch_matches(fixtures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    match_ids = [fixture['id'] for fixture in fixtures]
    match_data_list = fetch_match_data(match_ids)
    return match_data_list

def process_matches(match_data_list: List[Dict[str, Any]], date_str: str) -> List[Dict[str, Any]]:
    processed_matches = process_match_data(match_data_list, date_str)
    return processed_matches

def insert_matches(processed_matches: List[Dict[str, Any]]) -> str:
    insert_data_into_bigquery('match_data', processed_matches)
    return f"Inserted data for {len(processed_matches)} matches into BigQuery."

def send_discord_notification(title: str, message: str):
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook_url:
        print("Discord webhook URL not set.")
        return
    discord_data = {
        "content": None,
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": 65280 if "Success" in title else (16776960 if "No Fixtures" in title else 16711680)
            }
        ]
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.status_code}, {response.text}")
