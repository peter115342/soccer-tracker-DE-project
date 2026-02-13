import logging
import os
import json
from datetime import datetime
from google.cloud import pubsub_v1
from .utils.reddit_data_helper import (
    get_match_dates_from_bq,
    fetch_reddit_threads,
    save_to_gcs,
)
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)
from cloud_functions.data_contracts.reddit_contract import RedditDataContract
from cloud_functions.data_contracts.validation import validate_single


def fetch_reddit_data(event, context):
    """
    Cloud Function to fetch Reddit match threads from r/soccer and save to GCS bucket
    """
    try:
        dates = get_match_dates_from_bq()
        threads_processed = 0
        error_count = 0
        validation_error_count = 0

        for date in dates:
            try:
                reddit_data = fetch_reddit_threads(date)
                if reddit_data["threads"]:
                    try:
                        validate_single(reddit_data, RedditDataContract)
                    except Exception as ve:
                        validation_error_count += 1
                        logging.warning(f"Reddit data validation failed for date {date}: {ve}")
                        continue
                    save_to_gcs(reddit_data, date)
                    threads_processed += len(reddit_data["threads"])
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing date {date}: {e}")

        if validation_error_count > 0:
            validation_message = f"{validation_error_count} Reddit data records failed Pydantic validation and were skipped"
            logging.warning(validation_message)
            send_discord_notification(
                "⚠️ Fetch Reddit Data: Validation Issues", validation_message, 16776960
            )

        success_message = f"Processed {threads_processed} Reddit threads across {len(dates)} dates. Errors: {error_count}"
        logging.info(success_message)

        if threads_processed > 0:
            send_discord_notification(
                "✅ Fetch Reddit Data: Success", success_message, 65280
            )

            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "process_reddit_data_topic"
            )

            publish_data = {"action": "process_reddit"}
            future = publisher.publish(
                topic_path,
                data=json.dumps(publish_data).encode("utf-8"),
                timestamp=datetime.now().isoformat(),
            )

            publish_result = future.result()
            logging.info(f"Published trigger message with ID: {publish_result}")
        else:
            message = "No new Reddit threads found to process"
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
