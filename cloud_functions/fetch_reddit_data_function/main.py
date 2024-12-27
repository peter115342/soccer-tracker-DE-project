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
    """Cloud Function to fetch Reddit match thread data with enhanced tracking"""
    try:
        logging.info("Starting Reddit data fetch process")
        reddit = initialize_reddit()
        if not reddit:
            raise Exception("Failed to initialize Reddit client")
        logging.info("Reddit client initialized successfully")

        matches = get_processed_matches()
        logging.info(f"Retrieved {len(matches)} matches to process")

        if not matches:
            message = "No matches found to process"
            send_discord_notification(
                title="Reddit Data Fetch Status",
                message=message,
                color=16776960,  # Yellow
            )
            return message, 200

        processed_count = 0
        not_found_matches = []

        for match in matches:
            logging.info(f"Processing match ID: {match['match_id']}")
            thread_data = find_match_thread(reddit, match)

            if thread_data:
                save_to_gcs(thread_data, match["match_id"])
                processed_count += 1
                logging.info(f"Successfully processed match ID {match['match_id']}")
            else:
                match_info = (
                    f"{match['home_team']} vs {match['away_team']} "
                    f"({match['competition']}) on "
                    f"{match['utcDate'].strftime('%Y-%m-%d %H:%M UTC')}"
                )
                not_found_matches.append(match_info)
                logging.info(f"No thread found for: {match_info}")

        status_message = [
            f"Processed {processed_count} threads out of {len(matches)} matches.",
            f"Success rate: {(processed_count/len(matches))*100:.1f}%\n",
        ]

        if not_found_matches:
            status_message.extend(
                [
                    "Matches without threads found:",
                    *[f"• {match}" for match in not_found_matches],
                ]
            )
        else:
            status_message.append("All matches were successfully processed!")

        final_message = "\n".join(status_message)
        logging.info(final_message)

        if processed_count == len(matches):
            color = 65280  # Green for 100% success
        elif processed_count > 0:
            color = 16776960  # Yellow for partial success
        else:
            color = 15158332  # Red for no matches processed

        send_discord_notification(
            title="Reddit Data Fetch Results", message=final_message, color=color
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "reddit_to_parquet_topic"
        )

        publish_data = {
            "action": "convert_reddit",
            "timestamp": datetime.now().isoformat(),
            "processed_matches": processed_count,
            "total_matches": len(matches),
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )
        publish_result = future.result()
        logging.info(
            f"Published message to reddit_to_parquet_topic with ID: {publish_result}"
        )

        return final_message, 200

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
    """Sends a notification to Discord with the specified title, message, and color"""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set")
        return

    if len(message) > 1900:
        message_parts = [message[i : i + 1900] for i in range(0, len(message), 1900)]
        for i, part in enumerate(message_parts):
            discord_data = {
                "embeds": [
                    {
                        "title": f"{title} (Part {i+1}/{len(message_parts)})",
                        "description": part,
                        "color": color,
                    }
                ]
            }

            headers = {"Content-Type": "application/json"}
            response = requests.post(
                webhook_url, data=json.dumps(discord_data), headers=headers
            )

            if response.status_code != 204:
                logging.error(
                    f"Failed to send Discord notification part {i+1}: {response.status_code}, {response.text}"
                )
    else:
        discord_data = {
            "embeds": [{"title": title, "description": message, "color": color}]
        }

        headers = {"Content-Type": "application/json"}
        response = requests.post(
            webhook_url, data=json.dumps(discord_data), headers=headers
        )

        if response.status_code != 204:
            logging.error(
                f"Failed to send Discord notification: {response.status_code}, {response.text}"
            )
