import base64
import json
import os
import logging
import requests
import polars as pl
from google.cloud import storage, bigquery
from utils.bigquery_helpers_parquet_weather import load_weather_parquet_to_bigquery

def process_and_upload_weather_data(bucket_name, source_blob_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    json_data = json.loads(blob.download_as_text())

    if 'hourly' in json_data and 'visibility' in json_data['hourly']:
        del json_data['hourly']['visibility']

    df = pl.DataFrame(json_data['hourly'])
    tmp_parquet_file = '/tmp/weather.parquet'
    df.write_parquet(tmp_parquet_file)

    dest_blob = bucket.blob(destination_blob_name)
    dest_blob.upload_from_filename(tmp_parquet_file)

def load_weather_to_bigquery(event, context):
    """
    Background Cloud Function to be triggered by Pub/Sub.
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
        bigquery_client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
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

        source_blob_name = 'weather_data_json/weather.json'
        destination_blob_name = 'weather_data_parquet/weather.parquet'
        process_and_upload_weather_data(bucket_name, source_blob_name, destination_blob_name)

        weather_files = ['weather_data_parquet/weather.parquet']
        weather_loaded, weather_processed = load_weather_parquet_to_bigquery(
            bigquery_client,
            'sports_data',
            'weather_parquet',
            bucket_name,
            weather_files,
            job_config=job_config
        )

        status_message = (
            f"Processed 1 weather file\n"
            f"Successfully loaded: {weather_loaded} weather records\n"
            f"Weather file: {destination_blob_name}"
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
