import logging
import os
import json
import requests
from .utils.reddit_data_helper import (
    fetch_reddit_threads,
    save_to_gcs,
)


def fetch_reddit_data(event, context):
    """
    Cloud Function to fetch Reddit match threads from r/soccer and save to GCS bucket.
    This version processes only the hardcoded date '2024-10-26'.
    """
    try:
        date = "2024-10-26"

        threads_processed = 0
        error_count = 0

        try:
            reddit_data = fetch_reddit_threads(date)

            if reddit_data["threads"]:
                save_to_gcs(reddit_data, date)
                threads_processed += len(reddit_data["threads"])
            else:
                logging.info(f"No threads found for date {date}")

        except Exception as e:
            error_count += 1
            logging.error(f"Error processing date {date}: {e}")

        if threads_processed > 0:
            success_message = (
                f"Processed {threads_processed} Reddit threads for date {date}."
            )
            logging.info(success_message)
            send_discord_notification(
                "✅ Fetch Reddit Data: Success", success_message, 65280
            )
        else:
            message = f"No new Reddit threads found for date {date}"
            logging.info(message)
            send_discord_notification(
                "📝 Fetch Reddit Data: No New Threads", message, 16776960
            )

        return "Process completed successfully", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch Reddit Data: Failure", error_message, 16711680
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
