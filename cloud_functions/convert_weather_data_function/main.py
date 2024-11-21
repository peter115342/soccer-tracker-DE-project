import base64
import json
import os
import logging
import requests
from google.cloud import storage, pubsub_v1
import polars as pl

def transform_to_parquet(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(pubsub_message)

        if 'action' not in message_data or message_data['action'] != 'convert_weather':
            error_message = "Invalid message format or incorrect action"
            logging.error(error_message)
            send_discord_notification("❌ Convert Weather to Parquet: Invalid Trigger", error_message, 16711680)
            return error_message, 500

        bucket_name = os.environ.get('BUCKET_NAME')
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix='weather_data/')
        json_files = [blob.name for blob in blobs if blob.name.endswith('.json')]

        if not json_files:
            message = "No JSON files found in weather_data folder"
            logging.info(message)
            send_discord_notification("ℹ️ Convert Weather to Parquet: No Files", message, 16776960)
            return message, 200

        processed_count = 0
        skipped_count = 0
        error_count = 0

        for json_file in json_files:
            try:
                parquet_name = json_file.replace('.json', '.parquet')
                parquet_path = f"weather_data_parquet/{os.path.basename(parquet_name)}"
                parquet_blob = bucket.blob(parquet_path)

                if parquet_blob.exists():
                    logging.info(f"Parquet already exists for {json_file}")
                    skipped_count += 1
                    continue

                blob = bucket.blob(json_file)
                json_content = json.loads(blob.download_as_string())
                
                df = pl.DataFrame(json_content)
                
                df.write_parquet('/tmp/temp.parquet')
                parquet_blob.upload_from_filename('/tmp/temp.parquet')
                processed_count += 1
                
                logging.info(f"Converted {json_file} to parquet")

            except Exception as e:
                error_count += 1
                logging.error(f"Error converting {json_file}: {str(e)}")

        status_message = f"Processed: {processed_count}, Skipped: {skipped_count}, Errors: {error_count}"
        logging.info(status_message)

        if processed_count > 0:
            send_discord_notification("✅ Convert Weather to Parquet: Success", status_message, 65280)
            
            # Trigger BigQuery load
            publisher = pubsub_v1.PublisherClient()
            bigquery_topic_path = publisher.topic_path(os.environ['GCP_PROJECT_ID'], 'load_to_bigquery_topic')
            
            bigquery_message = {
                "action": "load_to_bigquery"
            }
            
            future = publisher.publish(
                bigquery_topic_path,
                data=json.dumps(bigquery_message).encode('utf-8')
            )
            
            publish_result = future.result()
            logging.info(f"Published trigger message to load_to_bigquery_topic with ID: {publish_result}")
        else:
            send_discord_notification("ℹ️ Convert Weather to Parquet: No New Files", status_message, 16776960)

        return status_message

    except Exception as e:
        error_message = f"Error in weather data conversion: {str(e)}"
        send_discord_notification("❌ Convert Weather to Parquet: Failure", error_message, 16711680)
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
                "color": color
            }
        ]
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        logging.error(f"Failed to send Discord notification: {response.status_code}, {response.text}")
