import logging
import json
import base64
import os
from datetime import datetime
from google.cloud import pubsub_v1
import requests
from .utils.standings_data_helper import (
    get_matches_data,
    fetch_standings_for_match,
    save_standings_to_gcs,
    get_processed_standings_dates,
    should_fetch_standings,
)


def fetch_standings_data(event, context):
    """
    Cloud Function to fetch standings data from top 5 leagues and save to bucket,
    with Discord notifications for success or failure and Pub/Sub trigger for next function.
    """
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "fetch_standings":
            error_message = "Invalid message format or action."
            logging.error(error_message)
            send_discord_notification(
                "❌ Fetch Standings Data: Invalid Trigger", error_message, 16711680
            )
            return error_message, 400

        matches_data = get_matches_data()
        processed_dates = get_processed_standings_dates()

        if not matches_data:
            message = "No matches found in matches_processed table."
            logging.info(message)
            send_discord_notification(
                "ℹ️ Fetch Standings Data: No Matches", message, 16776960
            )
            return "No matches to process.", 200

        processed_count = 0
        error_count = 0
        today = datetime.now().strftime("%Y-%m-%d")

        # Process unique date-competition combinations
        processed_combinations = set()

        for match in matches_data:
            combination = (match["date"], match["competition_code"])

            if combination not in processed_combinations and should_fetch_standings(
                match, processed_dates
            ):
                try:
                    standings_data = fetch_standings_for_match(match)
                    save_standings_to_gcs(
                        standings_data, match["date"], match["competition_code"]
                    )
                    processed_count += 1
                    processed_combinations.add(combination)
                except Exception as e:
                    error_count += 1
                    logging.error(
                        f"Error processing standings for {match['date']}: {e}"
                    )

        success_message = f"Processed standings for {processed_count} date-competition pairs. Errors: {error_count}"
        logging.info(success_message)
        send_discord_notification(
            "✅ Fetch Standings Data: Success", success_message, 65280
        )

        # Trigger next function via Pub/Sub
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
        logging.info(f"Published trigger message with ID: {publish_result}")

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
