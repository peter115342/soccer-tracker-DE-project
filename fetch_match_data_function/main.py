from flask import Request
import requests
import json
import os
import logging
from datetime import datetime, timedelta
from utils.match_data_helper import fetch_matches_for_competitions, save_to_gcs, validate_environment

def fetch_football_data(request: Request):
    """
    Cloud Function to fetch football match data from top 5 leagues and save to bucket,
    with Discord notifications for success or failure.
    """
    try:
        if not validate_environment():
            return "Missing required environment variables", 500

        date_to = datetime.now().strftime('%Y-%m-%d')
        date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        matches = fetch_matches_for_competitions(date_from, date_to)
        
        if not matches:
            message = "No matches found for the specified date range."
            send_discord_notification("⚠️ Fetch Match Data: No Matches", message, 16776960)
            return message, 200

        processed_count = 0
        error_count = 0
        
        for match in matches:
            try:
                match_id = match['id']
                save_to_gcs(match, match_id)
                processed_count += 1
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing match ID {match_id}: {e}")

        success_message = f"Processed {processed_count} matches successfully. Errors: {error_count}"
        send_discord_notification("✅ Fetch Match Data: Success", success_message, 65280)
        return success_message, 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification("❌ Fetch Match Data: Failure", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
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

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.status_code}, {response.text}")
