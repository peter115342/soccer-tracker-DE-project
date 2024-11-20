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
    source_blob_name = None
    try:
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(pubsub_message)
        
        if 'name' not in message_data:
            error_message = "No 'name' field in message data"
            logging.error(error_message)
            send_discord_notification("❌ Convert Weather to Parquet: Failure", error_message, 16711680)
            return error_message, 500
        
        source_blob_name = message_data['name']
        bucket_name = os.environ.get('BUCKET_NAME')
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        parquet_blob_name = source_blob_name.replace('.json', '.parquet')
        parquet_blob = bucket.blob(f'weather_data_parquet/{parquet_blob_name}')
        
        if parquet_blob.exists():
            message = f"Weather parquet file already exists for {source_blob_name}"
            logging.info(message)
            send_discord_notification("ℹ️ Convert Weather to Parquet: Skipped", message, 16776960)
            
            publisher = pubsub_v1.PublisherClient()
            next_topic_path = publisher.topic_path(os.environ['GCP_PROJECT_ID'], 'process_weather_data_topic')

            next_message = {
                "match_id": source_blob_name.replace('.json', ''),
                "status": "skipped"
            }

            future = publisher.publish(
                next_topic_path,
                data=json.dumps(next_message).encode('utf-8')
            )

            publish_result = future.result()
            logging.info(f"Published skip message to process_weather_data_topic with ID: {publish_result}")
            
            return message, 200
        
        logging.info(f"Starting conversion of {source_blob_name} to parquet")
        
        blob = bucket.blob(source_blob_name)
        json_content = json.loads(blob.download_as_string())
        
        df = pl.DataFrame(json_content)
        
        df.write_parquet('/tmp/temp.parquet')
        parquet_blob.upload_from_filename('/tmp/temp.parquet')
        
        success_message = f"Successfully converted weather data {source_blob_name} to parquet"
        logging.info(success_message)
        send_discord_notification("✅ Convert Weather to Parquet: Success", success_message, 65280)

        publisher = pubsub_v1.PublisherClient()
        next_topic_path = publisher.topic_path(os.environ['GCP_PROJECT_ID'], 'process_weather_data_topic')

        next_message = {
            "match_id": source_blob_name.replace('.json', ''),
            "status": "success"
        }

        future = publisher.publish(
            next_topic_path,
            data=json.dumps(next_message).encode('utf-8')
        )

        publish_result = future.result()
        logging.info(f"Published message to process_weather_data_topic with ID: {publish_result}")
        
        return success_message

    except Exception as e:
        error_message = f"Error converting weather data {source_blob_name if source_blob_name else 'unknown file'} to parquet: {str(e)}"
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
