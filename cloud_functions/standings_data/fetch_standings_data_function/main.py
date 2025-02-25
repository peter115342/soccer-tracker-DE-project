import logging
import json
import base64
import os
from datetime import datetime
from google.cloud import pubsub_v1
from .utils.standings_data_helper import (
    get_unique_dates,
    fetch_standings_for_date,
    save_standings_to_gcs,
    get_processed_standings_dates,
)
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def fetch_standings_data(event, context):
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "fetch_standings":
            raise ValueError("Invalid message format or action")

        unique_dates = get_unique_dates()
        processed_dates = get_processed_standings_dates()

        dates_to_process = [
            date for date in unique_dates if date not in processed_dates
        ]

        if not dates_to_process:
            message = "No new dates to process"
            logging.info(message)
            send_discord_notification(
                "ℹ️ Fetch Standings Data: No New Dates", message, 16776960
            )
            return "No new data to process", 200

        processed_count = 0
        error_count = 0

        for date in dates_to_process:
            try:
                standings_list = fetch_standings_for_date(date)

                for standings in standings_list:
                    competition_id = standings["competitionId"]
                    save_standings_to_gcs(standings, date, competition_id)
                    processed_count += 1

            except Exception as e:
                error_count += 1
                logging.error(f"Error processing date {date}: {str(e)}")

        success_message = (
            f"Processed {len(dates_to_process)} new dates\n"
            f"Total standings entries: {processed_count}\n"
            f"Errors: {error_count}"
        )

        logging.info(success_message)
        send_discord_notification(
            "✅ Fetch Standings Data: Complete", success_message, 65280
        )

        if processed_count > 0:
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

            future.result()

        return "Process completed successfully.", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch Standings Data: Failure", error_message, 16711680
        )
        logging.exception(error_message)
        return error_message, 500
