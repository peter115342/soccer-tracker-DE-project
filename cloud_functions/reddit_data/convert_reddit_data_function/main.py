import base64
import json
import os
import logging
import requests
from google.cloud import storage, pubsub_v1
import polars as pl


def transform_to_parquet(event, context):
    """Background Cloud Function to convert Reddit data JSON files to Parquet format.
    Args:
         event (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if "action" not in message_data or message_data["action"] != "convert_reddit":
            error_message = "Invalid message format or missing action 'convert_reddit'"
            logging.error(error_message)
            send_discord_notification(
                "❌ Reddit Convert to Parquet: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        bucket_name = os.environ.get("BUCKET_NAME")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix="reddit_data/")
        json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

        if not json_files:
            message = "No Reddit JSON files found to convert"
            logging.info(message)
            send_discord_notification(
                "📝 Reddit Convert to Parquet: No Files", message, 16776960
            )

            publisher = pubsub_v1.PublisherClient()
            next_topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "reddit_to_bigquery_topic"
            )

            next_message = {"action": "load_reddit_to_bigquery"}

            future = publisher.publish(
                next_topic_path, data=json.dumps(next_message).encode("utf-8")
            )

            publish_result = future.result()
            logging.info(
                f"Published no-files message to reddit_to_bigquery_topic with ID: {publish_result}"
            )

            return message, 200

        processed_count = 0
        skipped_count = 0

        for json_file in json_files:
            parquet_name = json_file.replace(".json", ".parquet")
            parquet_path = f"reddit_data_parquet/{os.path.basename(parquet_name)}"
            parquet_blob = bucket.blob(parquet_path)

            if parquet_blob.exists():
                skipped_count += 1
                logging.info(f"Parquet already exists for {json_file}")
                continue

            blob = bucket.blob(json_file)
            json_content = json.loads(blob.download_as_string())

            df = pl.DataFrame([json_content])

            df.write_parquet("/tmp/temp.parquet")
            parquet_blob.upload_from_filename("/tmp/temp.parquet")
            processed_count += 1

        status_message = (
            f"Processed {processed_count} files, skipped {skipped_count} existing files"
        )
        logging.info(status_message)
        send_discord_notification(
            "✅ Reddit Convert to Parquet: Complete", status_message, 65280
        )

        publisher = pubsub_v1.PublisherClient()
        next_topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "reddit_to_bigquery_topic"
        )

        next_message = {"action": "load_reddit_to_bigquery"}

        future = publisher.publish(
            next_topic_path, data=json.dumps(next_message).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to reddit_to_bigquery_topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Reddit data conversion to Parquet: {str(e)}"
        send_discord_notification(
            "❌ Reddit Convert to Parquet: Failure", error_message, 16711680
        )
        logging.exception(error_message)
        return error_message, 500


def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return

    discord_data = {
        "content": None,
        "embeds": [{"title": title, "description": message, "color": color}],
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(
        webhook_url, data=json.dumps(discord_data), headers=headers
    )
    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
