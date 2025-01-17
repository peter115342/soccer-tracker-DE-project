import logging
import os
import base64
import json
from google.cloud import storage, pubsub_v1
from datetime import datetime
from .utils.reddit_processor import process_reddit_data
import requests


def process_reddit_threads(event, context):
    """Cloud Function to process all available Reddit threads and match them with actual matches"""
    try:
        if event and "data" in event:
            pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
            message_data = json.loads(pubsub_message)

            if (
                "action" not in message_data
                or message_data["action"] != "process_reddit"
            ):
                error_message = (
                    "Invalid message format or missing action 'process_reddit'"
                )
                logging.error(error_message)
                send_discord_notification(
                    "❌ Process Reddit Data: Invalid Trigger", error_message, 16711680
                )
                return error_message, 500

        bucket_name = os.environ.get("BUCKET_NAME")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix="reddit_data/raw/")
        dates = [
            blob.name.split("/")[-1].replace(".json", "")
            for blob in blobs
            if blob.name.endswith(".json")
        ]

        total_processed = 0
        total_skipped = 0
        all_validations = []

        for date in dates:
            result = process_reddit_data(date, bucket_name)
            total_processed += result["processed_threads"]
            total_skipped += result["skipped_threads"]
            all_validations.extend(result["validations"])

        validation_summary = []
        for validation in all_validations:
            if validation["valid"]:
                validation_summary.append(
                    f"Match {validation['match_id']}: ✅ {', '.join(validation['passed'])}"
                )
            else:
                validation_summary.append(
                    f"Match {validation['match_id']}: "
                    f"✅ {', '.join(validation['passed'])} "
                    f"❌ {', '.join(validation['failed'])}"
                )

        success_message = (
            f"Successfully processed {total_processed} new threads "
            f"across {len(dates)} dates and skipped {total_skipped} threads\n\n"
            f"Validation Results:\n" + "\n".join(validation_summary)
        )

        send_discord_notification(
            "✅ Process Reddit Data: Success", success_message, 65280
        )

        if total_processed > 0:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "convert_reddit_to_parquet_topic"
            )

            next_message = {
                "action": "convert_reddit",
                "timestamp": datetime.now().isoformat(),
                "processed_threads": total_processed,
            }

            future = publisher.publish(
                topic_path, data=json.dumps(next_message).encode("utf-8")
            )

            publish_result = future.result()
            logging.info(
                f"Published message to convert_reddit_data_topic with ID: {publish_result}"
            )

        return "Processing completed successfully", 200

    except Exception as e:
        error_message = f"An error occurred while processing Reddit threads: {str(e)}"
        send_discord_notification(
            "❌ Process Reddit Data: Error", error_message, 16711680
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
        webhook_url, data=json.dumps(discord_data), headers=headers, timeout=90
    )

    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
