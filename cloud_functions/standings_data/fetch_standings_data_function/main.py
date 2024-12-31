import requests
import json
import os
import logging
from datetime import datetime
from google.cloud import pubsub_v1
from .utils.standings_helper import fetch_standings_for_competitions, save_to_gcs


def fetch_standings_data(event, context):
    """
    Cloud Function to fetch standings data from top 5 leagues and save to bucket,
    with Discord notifications for success or failure and conditional Pub/Sub trigger.
    """
    try:
        logging.info("Starting standings data fetch")
        standings = fetch_standings_for_competitions()

        new_standings = 0
        error_count = 0
        processed_standings = []

        if not standings:
            message = "No new standings data to fetch. No changes in matches_processed."
            logging.info(message)
            send_discord_notification(
                "ℹ️ Fetch Standings Data: No Updates", message, 16776960
            )
            return "No new standings to process.", 200

        for standing in standings:
            try:
                competition_id = standing["competition"]["id"]
                save_to_gcs(standing, competition_id)
                processed_standings.append(standing)
                new_standings += 1
            except Exception as e:
                error_count += 1
                logging.error(
                    f"Error processing standings for competition ID {competition_id}: {e}"
                )

        success_message = (
            f"Fetched {new_standings} standings updates. Errors: {error_count}."
        )
        logging.info(success_message)
        send_discord_notification(
            "✅ Fetch Standings Data: Success", success_message, 65280
        )

        if new_standings > 0:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "convert_standings_to_parquet_topic"
            )

            publish_data = {"action": "convert_standings"}

            future = publisher.publish(
                topic_path,
                data=json.dumps(publish_data).encode("utf-8"),
                timestamp=datetime.now().isoformat(),
            )

            publish_result = future.result()
            logging.info(
                f"Published trigger message to convert_standings_to_parquet_topic with ID: {publish_result}"
            )

        return "Process completed.", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch Standings Data: Failure", error_message, 16711680
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
