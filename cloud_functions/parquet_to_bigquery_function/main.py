import base64
import json
import os
import logging
import requests
from google.cloud import storage, bigquery
from utils.bigquery_helpers_parquet import load_match_parquet_to_bigquery, load_weather_parquet_to_bigquery

def load_to_bigquery(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Loads Parquet files from GCS to BigQuery tables.
    """
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(pubsub_message)

        if 'action' not in message_data or message_data['action'] != 'load_to_bigquery':
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification("❌ BigQuery Load: Invalid Trigger", error_message, 16711680)
            return error_message, 500

        bucket_name = os.environ.get('BUCKET_NAME')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bigquery_client = bigquery.Client()

        match_files = [blob.name for blob in bucket.list_blobs(prefix='match_data_parquet/')]
        weather_files = [blob.name for blob in bucket.list_blobs(prefix='weather_data_parquet/')]

        match_loaded, match_processed = load_match_parquet_to_bigquery(
            bigquery_client,
            os.environ.get('MATCH_DATASET'),
            os.environ.get('MATCH_TABLE'),
            bucket_name,
            match_files
        )

        weather_loaded, weather_processed = load_weather_parquet_to_bigquery(
            bigquery_client,
            os.environ.get('WEATHER_DATASET'),
            os.environ.get('WEATHER_TABLE'),
            bucket_name,
            weather_files
        )

        status_message = (
            f"Loaded {match_loaded} new match files and {weather_loaded} new weather files to BigQuery\n"
            f"Match files: {', '.join(match_processed)}\n"
            f"Weather files: {', '.join(weather_processed)}"
        )
        
        logging.info(status_message)
        send_discord_notification("✅ BigQuery Load: Complete", status_message, 65280)
        
        return status_message, 200

    except Exception as e:
        error_message = f"Error during BigQuery load: {str(e)}"
        send_discord_notification("❌ BigQuery Load: Failure", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
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

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        logging.error(f"Failed to send Discord notification: {response.status_code}, {response.text}")
