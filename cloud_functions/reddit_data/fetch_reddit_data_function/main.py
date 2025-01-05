import logging
from google.cloud import pubsub_v1
import json
import os
from datetime import datetime
import requests
import time
from .utils.reddit_data_helper import (
    initialize_reddit,
    get_processed_matches,
    fetch_threads_for_date,
    extract_thread_data,
    save_to_gcs,
    calculate_thread_match_score,
)


def send_discord_notification(title: str, message: str, color: int):
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
            response = requests.post(webhook_url, json=discord_data, headers=headers)
            if response.status_code != 204:
                logging.error(
                    f"Failed to send Discord notification part {i+1}: {response.status_code}, {response.text}"
                )
    else:
        discord_data = {
            "embeds": [{"title": title, "description": message, "color": color}]
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(webhook_url, json=discord_data, headers=headers)
        if response.status_code != 204:
            logging.error(
                f"Failed to send Discord notification: {response.status_code}, {response.text}"
            )


def fetch_reddit_data(event, context):
    try:
        logging.info("Starting Reddit data fetch process")
        reddit = initialize_reddit()
        if not reddit:
            raise Exception("Failed to initialize Reddit client")
        logging.info("Reddit client initialized successfully")

        matches_by_date = get_processed_matches()
        total_matches = sum(len(matches) for matches in matches_by_date.values())

        if not total_matches:
            message = "📝 No matches found to process"
            send_discord_notification(
                title="Reddit Data Fetch Status",
                message=message,
                color=16776960,  # Yellow
            )
            processed_count = 0
            final_message = message
        else:
            processed_count = 0
            not_found_matches = []

            for date, matches in matches_by_date.items():
                try:
                    logging.info(f"Processing matches for date: {date}")
                    threads = fetch_threads_for_date(reddit, date)
                    logging.info(f"Found {len(threads)} threads for date {date}")

                    for match in matches:
                        best_thread = None
                        best_score = 0

                        for thread in threads:
                            score = calculate_thread_match_score(thread, match, date)
                            if score > 40 and score > best_score:
                                best_score = score
                                best_thread = thread

                        if best_thread:
                            thread_data = extract_thread_data(
                                best_thread, match["match_id"]
                            )
                            if save_to_gcs(thread_data, match["match_id"]):
                                processed_count += 1
                                logging.info(
                                    f"Successfully processed match ID {match['match_id']}"
                                )
                        else:
                            match_info = (
                                f"{match['home_team']} vs {match['away_team']} "
                                f"({match['competition']}) on "
                                f"{match['utcDate'].strftime('%Y-%m-%d %H:%M UTC')}"
                            )
                            not_found_matches.append((match_info, date))
                            logging.info(f"No thread found for: {match_info}")

                    time.sleep(2)  # Rate limiting protection between dates

                except Exception as e:
                    if (
                        hasattr(e, "response")
                        and getattr(e.response, "status_code", None) == 429
                    ):
                        logging.info("Rate limit hit, waiting 180 seconds...")
                        time.sleep(180)
                        continue
                    else:
                        raise e

            success_count = processed_count
            failure_count = total_matches - success_count
            success_rate = (success_count / total_matches) * 100 if total_matches else 0

            status_message = [
                f"Processed {total_matches} matches.",
                f"Successfully processed {success_count} threads.",
                f"Failed to process {failure_count} matches.",
                f"Success rate: {success_rate:.1f}%\n",
            ]

            if not_found_matches:
                status_message.extend(
                    [
                        "Matches without threads found:",
                        *[f"• {match}" for match, _ in not_found_matches],
                    ]
                )
            else:
                status_message.append("All matches were successfully processed!")

            final_message = "\n".join(status_message)
            logging.info(final_message)

            if success_count == total_matches:
                color = 65280  # Green
            elif success_count > 0:
                color = 16776960  # Yellow
            else:
                color = 15158332  # Red

            send_discord_notification(
                title="Reddit Data Fetch Results",
                message=final_message,
                color=color,
            )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "convert_reddit_to_parquet_topic"
        )

        logging.info(f"Publishing to topic path: {topic_path}")
        publish_data = {
            "action": "convert_reddit",
            "timestamp": datetime.now().isoformat(),
            "processed_matches": processed_count,
            "total_matches": total_matches,
        }
        logging.info(f"Publishing data: {json.dumps(publish_data)}")

        try:
            future = publisher.publish(
                topic_path, data=json.dumps(publish_data).encode("utf-8")
            )
            publish_result = future.result(timeout=30)
            logging.info(f"Successfully published message with ID: {publish_result}")
        except Exception as pub_error:
            logging.error(f"Failed to publish message: {str(pub_error)}")
            raise

        return final_message, 200

    except Exception as e:
        error_message = f"Error fetching Reddit data: {str(e)}"
        logging.exception(error_message)

        send_discord_notification(
            title="Reddit Data Fetch Failed",
            message=error_message,
            color=15158332,  # Red
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "convert_reddit_to_parquet_topic"
        )

        error_publish_data = {
            "action": "convert_reddit",
            "timestamp": datetime.now().isoformat(),
            "processed_matches": 0,
            "total_matches": 0,
            "error": error_message,
        }

        try:
            future = publisher.publish(
                topic_path, data=json.dumps(error_publish_data).encode("utf-8")
            )
            future.result(timeout=30)
        except Exception as pub_error:
            logging.error(f"Failed to publish error message: {str(pub_error)}")

        return error_message, 500
