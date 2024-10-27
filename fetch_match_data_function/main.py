from typing import List, Dict, Any
from flask import Request
from datetime import datetime
import requests
import json
import os
import logging

from utils.api_helpers_match import fetch_matches_by_date_range
from utils.data_processing import process_match_data
from utils.bigquery_helpers_match import insert_data_into_bigquery

def fetch_football_data(request: Request):
    """
    Cloud Function to fetch football match data and load into BigQuery,
    with Discord notifications for success or failure.
    """
    try:
        date_from, date_to = get_date_range(request)
        logging.info(f"Fetching matches between {date_from} and {date_to}.")
        matches = fetch_matches_by_date_range(date_from, date_to)

        if not matches:
            message = f"No finished matches found on {date_from}."
            send_discord_notification("⚠️ Fetch Football Data: No Matches", message, 16776960)  # Yellow
            return message, 200

        processed_matches = process_match_data(matches)
        result = insert_matches(processed_matches)

        if result['inserted_count'] == 0:
            success_message = f"No new matches to insert into BigQuery. Skipped {result['skipped_count']} matches that already existed."
        else:
            success_message = f"Inserted data for {result['inserted_count']} new matches into BigQuery. Skipped {result['skipped_count']} matches that already existed."

        send_discord_notification("✅ Fetch Football Data: Success", success_message, 65280)  # Green
        return success_message, 200
    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification("❌ Fetch Football Data: Failure", error_message, 16711680)  # Red
        logging.exception(error_message)
        return error_message, 500

def get_date_range(request: Request) -> tuple[str, str]:
    request_args = request.args
    if request_args and 'dateFrom' in request_args and 'dateTo' in request_args:
        date_from = request_args['dateFrom']
        date_to = request_args['dateTo']
    else:
        today = datetime.now()
        date_from = today.strftime('%Y-%m-%d')
        date_to = date_from
    return date_from, date_to

def insert_matches(processed_matches: List[Dict[str, Any]]) -> Dict[str, int]:
    result = insert_data_into_bigquery('match_data', processed_matches)
    return result

def send_discord_notification(title: str, message: str, color: int):
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
                "color": color
            }
        ]
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.status_code}, {response.text}")
