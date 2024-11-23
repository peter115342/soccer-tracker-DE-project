import base64
import json
import os
import logging
import requests
from google.cloud import storage, bigquery
from utils.bigquery_helpers_parquet_weather import load_weather_parquet_to_bigquery

def load_weather_to_bigquery(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Loads Weather Parquet files from GCS to BigQuery tables.
    """
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(pubsub_message)

        if 'action' not in message_data or message_data['action'] != 'load_weather_to_bigquery':
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification("❌ Weather BigQuery Load: Invalid Trigger", error_message, 16711680)
            return error_message, 500

        bucket_name = os.environ.get('BUCKET_NAME')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bigquery_client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND

        )

        dataset_ref = bigquery_client.dataset('sports_data')
        try:
            bigquery_client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            bigquery_client.create_dataset(dataset)

        weather_files = [blob.name for blob in bucket.list_blobs(prefix='weather_data_parquet/')]
        weather_loaded, weather_processed = load_weather_parquet_to_bigquery(
            bigquery_client,
            'sports_data',
            'weather_parquet',
            bucket_name,
            weather_files,
            job_config=job_config
        )

        status_message = (
            f"Processed {len(weather_files)} weather files\n"
            f"Successfully loaded: {weather_loaded} weather records\n"
            f"Weather files: {', '.join(weather_processed)}"
        )

        logging.info(status_message)
        send_discord_notification("✅ Weather BigQuery Load: Complete", status_message, 65280)

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Weather BigQuery load: {str(e)}"
        logging.exception(error_message)
        send_discord_notification("❌ Weather BigQuery Load: Failure", error_message, 16711680)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
    if not webhook_url:
        logging.error("Discord webhook URL not configured")
        return

    if len(message) > 2000:
        message = message[:1997] + "..."

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

    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            webhook_url, 
            data=json.dumps(discord_data), 
            headers=headers, 
            timeout=10
        )
        response.raise_for_status()
        if response.status_code != 204:
            logging.error(f"Discord notification failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Discord notification failed: {str(e)}")
