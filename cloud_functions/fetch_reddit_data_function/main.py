import logging
from google.cloud import pubsub_v1
import json
import os
from datetime import datetime
import requests
from .utils.reddit_data_helper import (
    initialize_reddit,
    get_processed_matches,
    find_match_thread,
    save_to_gcs,
)


def fetch_reddit_data(event, context):
    """Cloud Function to fetch Reddit match thread data"""
    try:
        logging.info("Starting Reddit data fetch process")
        reddit = initialize_reddit()
        if not reddit:
            raise Exception("Failed to initialize Reddit client")
        logging.info("Reddit client initialized successfully")

        matches = get_processed_matches()
        logging.info(f"Retrieved {len(matches)} matches to process")

        if not matches:
            send_discord_notification(
                title="Reddit Data Fetch Warning",
                message="No matches found to process",
                color=16776960,  # Yellow
            )
            return "No matches found", 200
        processed_count = 0
        not_found_count = 0

        for match in matches:
            logging.info(f"Processing match ID: {match['match_id']}")
            thread_data = find_match_thread(reddit, match)

            if thread_data:
                save_to_gcs(thread_data, match["match_id"])
                processed_count += 1
                logging.info(f"Successfully processed match ID {match['match_id']}")
            else:
                not_found_count += 1
                logging.info(
                    f"No matching thread found for match ID {match['match_id']}"
                )

        status_message = (
            f"Processed {processed_count} threads. Not found: {not_found_count}"
        )
        logging.info(status_message)

        send_discord_notification(
            title="Reddit Data Fetch Successful",
            message=status_message,
            color=3066993,  # Green
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "reddit_to_parquet_topic"
        )

        publish_data = {
            "action": "convert_reddit",
            "timestamp": datetime.now().isoformat(),
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to reddit_to_parquet_topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error fetching Reddit data: {str(e)}"
        logging.exception(error_message)

        send_discord_notification(
            title="Reddit Data Fetch Failed",
            message=error_message,
            color=15158332,  # Red
        )

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
