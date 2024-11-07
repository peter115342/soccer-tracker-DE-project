from flask import Request
import requests
import json
import os
import logging

from utils.data_processing import get_json_files_from_gcs, process_match_data, transform_to_bigquery_rows
from utils.bigquery_helpers_match import insert_data_into_bigquery

def process_football_data(request: Request):
    """
    Cloud Function to process football match data from GCS and load into BigQuery,
    with Discord notifications for success or failure.
    """
    try:
        bucket_name = os.environ.get('BUCKET_NAME')
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

        logging.info("Starting to process football data from GCS")

        match_data = get_json_files_from_gcs(bucket_name, project_id)

        if not match_data:
            message = "No match data found in GCS bucket."
            send_discord_notification("⚠️ Process Football Data: No Data", message, 16776960)  # Yellow
            return message, 200

        df = process_match_data(match_data)

        bq_rows = transform_to_bigquery_rows(df)

        result = insert_data_into_bigquery('match_data', bq_rows)

        if result['inserted_count'] == 0:
            success_message = f"No new matches to insert into BigQuery. Skipped {result['skipped_count']} matches that already existed."
        else:
            success_message = f"Processed {len(df)} matches. Inserted {result['inserted_count']} new matches. Skipped {result['skipped_count']} existing matches."

        send_discord_notification("✅ Process Football Data: Success", success_message, 65280)  # Green
        return success_message, 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification("❌ Process Football Data: Failure", error_message, 16711680)  # Red
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

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        print(f"Failed to send Discord notification: {response.status_code}, {response.text}")

