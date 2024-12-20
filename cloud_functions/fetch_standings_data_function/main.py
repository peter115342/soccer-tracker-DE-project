import logging
import json
import base64
import os
from datetime import datetime
from google.cloud import pubsub_v1
import requests
from .utils.standings_data_helper import (
    get_unique_dates,
    fetch_standings_for_date,
    save_standings_to_gcs,
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
        unique_dates = get_unique_dates()
        if not unique_dates:
            message = "No match dates found in matches_processed table."
            logging.info(message)
            send_discord_notification(
                "ℹ️ Fetch Standings Data: No Dates", message, 16776960
            )
            return "No dates to process.", 200

        processed_dates = 0
        error_count = 0

        for date in unique_dates:
            standings_list = fetch_standings_for_date(date)

            for standings in standings_list:
                try:
                    competition_code = standings["competitionCode"]
                    save_standings_to_gcs(standings, date, competition_code)
                    processed_dates += 1
                except Exception as e:
                    error_count += 1
                    logging.error(f"Error processing standings for {date}: {e}")

        success_message = f"Processed standings for {processed_dates} date-competition pairs. Errors: {error_count}"
        logging.info(success_message)
        send_discord_notification(
            "✅ Fetch Standings Data: Success", success_message, 65280
        )

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
