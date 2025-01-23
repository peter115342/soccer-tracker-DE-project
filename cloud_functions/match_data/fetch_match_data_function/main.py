import requests
import json
import os
import logging
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
from .utils.match_data_helper import fetch_matches_for_competitions, save_to_gcs


def fetch_football_data(event, context):
    """
    Cloud Function to fetch football match data from top 5 leagues and save to bucket,
    with Discord notifications for success or failure and Pub/Sub trigger for next function.
    """
    try:
        date_to = datetime.now().strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        logging.info(f"Fetching matches from {date_from} to {date_to}")
        matches = fetch_matches_for_competitions(date_from, date_to)

        new_matches = 0
        error_count = 0
        processed_matches = []

        if not matches:
            message = (
                f"No new matches found on date {date_to}. Skipping conversion step."
            )
            logging.info(message)
            send_discord_notification(
                "📝 Fetch Match Data: No New Matches", message, 16776960
            )
            return "No new matches to process.", 200

        for match in matches:
            try:
                match_id = match["id"]
                if "season" in match and "winner" in match["season"]:
                    match["season"]["winner"] = None
                save_to_gcs(match, match_id)
                processed_matches.append(match)
                new_matches += 1
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing match ID {match_id}: {e}")

        success_message = f"Fetched {new_matches} new matches. Errors: {error_count}. Triggering conversion step."
        logging.info(success_message)
        send_discord_notification(
            "✅ Fetch Match Data: Success", success_message, 65280
        )

        if new_matches > 0:
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "convert_to_parquet_topic"
            )

            publish_data = {"action": "convert_matches"}

            future = publisher.publish(
                topic_path,
                data=json.dumps(publish_data).encode("utf-8"),
                timestamp=datetime.now().isoformat(),
            )

            publish_result = future.result()
            logging.info(
                f"Published trigger message to convert_to_parquet_topic with ID: {publish_result}"
            )

        return "Process completed.", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch Match Data: Failure", error_message, 16711680
        )
        logging.exception(error_message)
        return error_message, 500


def send_discord_notification(title: str, message: str, color: int):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
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
            }
        ],
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        webhook_url, data=json.dumps(discord_data), headers=headers, timeout=90
    )
    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
