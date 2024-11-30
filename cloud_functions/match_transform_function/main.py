import base64
import json
import os
import logging
import requests
from pathlib import Path
from google.cloud import bigquery, pubsub_v1
from datetime import datetime


def load_sql_query():
    sql_path = Path(__file__).parent / "sql" / "transform_matches.sql"
    with open(sql_path, "r") as f:
        return f.read()


def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.error("Discord webhook URL not configured")
        return

    if len(message) > 2000:
        message = message[:1997] + "..."

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

    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(
            webhook_url, data=json.dumps(discord_data), headers=headers, timeout=10
        )
        response.raise_for_status()
        if response.status_code != 204:
            logging.error(
                f"Discord notification failed with status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        logging.error(f"Discord notification failed: {str(e)}")


def transform_matches(event, context):
    """Cloud Function to transform match data into native BigQuery table"""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "transform_matches":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Match Transform: Invalid Trigger", error_message, 16711680
            )
            return error_message, 400

        client = bigquery.Client()
        transformation_query = load_sql_query()

        job_config = bigquery.QueryJobConfig(
            destination=f"{os.environ['GCP_PROJECT_ID']}.sports_data_eu.matches_processed",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="utcDate"
            ),
        )

        query_job = client.query(transformation_query, job_config=job_config)
        result = query_job.result()  # noqa: F841

        count_query = "SELECT COUNT(*) as match_count FROM sports_data_raw_parquet.matches_processed"
        count_job = client.query(count_query)
        match_count = next(count_job.result())[0]

        status_message = (
            f"Match transformation completed successfully.\n"
            f"Total matches processed: {match_count}"
        )

        logging.info(status_message)
        send_discord_notification("✅ Match Transform: Success", status_message, 65280)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "fetch_weather_data_topic"
        )

        publish_data = {
            "action": "fetch_weather",
            "timestamp": datetime.now().isoformat(),
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to fetch-weather-data-topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error transforming match data: {str(e)}"
        logging.error(error_message)
        send_discord_notification(
            "❌ Match Transform: Failure", error_message, 16711680
        )
        return error_message, 500
