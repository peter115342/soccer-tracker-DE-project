import requests
import json
import os
import logging
import base64
from google.auth import default
from utils.data_processing_match import get_json_files_from_gcs, process_match_data, transform_to_bigquery_rows
from utils.bigquery_helpers_match import insert_data_into_bigquery
from google.cloud import pubsub_v1
from datetime import datetime


def process_football_data(event):
    """
    Cloud Function to process new football match data from GCS and load into BigQuery,
    triggered by Pub/Sub message from fetch_match_data function.
    """
    try:

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        input_data = json.loads(pubsub_message)
        logging.info(f"Received input data from fetch_match_data: {input_data}")

        bucket_name = os.environ.get('BUCKET_NAME')
        _, project_id = default()

        if project_id is None:
           project_id = os.environ.get('GCP_PROJECT_ID')
           
        if bucket_name is None:
            raise ValueError("BUCKET_NAME environment variable is not set")

        logging.info("Starting to process new football data from GCS")

        match_data = input_data.get('matches', [])
        if not match_data:
            match_data = get_json_files_from_gcs(bucket_name, project_id)

        if not match_data:
            message = "No new match data files found for processing."
            send_discord_notification(
                "📝 Football Data Processing: No Updates", 
                "All match data files have already been processed. No new updates required.", 
                16776960
            )
            return message, 200

        df = process_match_data(match_data)
        if df.is_empty():
            message = "No valid matches found in the new data files."
            send_discord_notification(
                "⚠️ Football Data Processing: Empty Data", 
                "New files were found but contained no valid match data.", 
                16776960
            )
            return message, 200

        bq_rows = transform_to_bigquery_rows(df)
        result = insert_data_into_bigquery('match_data', bq_rows)

        if result['inserted_count'] == 0:
            notification_title = "ℹ️ Football Data Processing: No New Matches"
            success_message = (
                f"⚠️ Found {len(match_data)} new files.\n"
                f"All {result['skipped_count']} matches already exist in the database."
            )
            color = 16776960
        else:
            notification_title = "✅ Football Data Processing: Success"
            success_message = (
                f"Successfully processed {len(match_data)} new files:\n"
                f"• {result['inserted_count']} new matches added\n"
                f"• {result['skipped_count']} existing matches skipped"
            )
            color = 65280

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, 'fetch_weather_data_topic')
        
        publish_data = {
            "processed_matches": result,
            "stats": input_data.get('stats', {}),
            "timestamp": datetime.now().isoformat()
        }
        
        future = publisher.publish(
            topic_path,
            data=json.dumps(publish_data).encode('utf-8')
        )
        publish_result = future.result()
        logging.info(f"Published message to fetch_weather_data_topic with ID: {publish_result}")

        send_discord_notification(notification_title, success_message, color)
        return success_message, 200

    except Exception as e:
        error_message = f"Error during football data processing: {str(e)}"
        send_discord_notification(
            "❌ Football Data Processing: Error", 
            f"Processing failed with error:\n{error_message}", 
            16711680
        )
        logging.exception(error_message)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a formatted notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook_url:
        logging.warning("Discord webhook URL not configured - notifications disabled")
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
        logging.error(f"Discord notification failed: Status {response.status_code}, Response: {response.text}")
