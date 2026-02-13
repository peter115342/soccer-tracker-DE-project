import logging
import os
import json
import base64
from pathlib import Path
from .utils.weather_data_helper import fetch_weather_by_coordinates, save_weather_to_gcs
from google.cloud import bigquery, pubsub_v1, storage
from datetime import datetime
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)
from cloud_functions.data_contracts.weather_contract import WeatherContract
from cloud_functions.data_contracts.validation import validate_single

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def load_query(name: str) -> str:
    """Load SQL query from file."""
    sql_path = Path(__file__).parent / "sql" / f"{name}.sql"
    return sql_path.read_text()


def fetch_weather_data(data, context):
    """Fetches and stores weather data for football match locations in GCS."""
    try:
        if isinstance(data, str):
            input_data = json.loads(data)
        else:
            input_data = json.loads(base64.b64decode(data["data"]).decode("utf-8"))

        if "action" not in input_data or input_data["action"] != "fetch_weather":
            error_message = "Invalid message format or incorrect action"
            logging.error(error_message)
            send_discord_notification(
                "❌ Weather Data: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        logging.info(f"Received processed match data: {input_data}")

        match_data = get_match_data()

        if not match_data:
            message = "📝 No matches to fetch weather data for"
            logging.info(message)
            send_discord_notification("Weather Data Update", message, 16776960)

            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(
                GCP_PROJECT_ID, "convert_weather_to_parquet_topic"
            )

            publish_data = {
                "weather_data": [],
                "stats": {
                    "processed_count": 0,
                    "error_count": 0,
                    "timestamp": datetime.now().isoformat(),
                },
            }

            future = publisher.publish(
                topic_path, data=json.dumps(publish_data).encode("utf-8")
            )

            publish_result = future.result()
            logging.info(
                f"Published empty message to convert-weather-to-parquet-topic with ID: {publish_result}"
            )

            return "No matches to process weather data for.", 200

        processed_count = 0
        error_count = 0
        validation_error_count = 0
        processed_weather_data = []
        storage_client = storage.Client(project=GCP_PROJECT_ID)
        bucket = storage_client.bucket(GCS_BUCKET_NAME)

        for match in match_data:
            try:
                match_id = match["id"]

                blob = bucket.blob(f"weather_data/{match_id}.json")
                if blob.exists():
                    logging.info(
                        f"Weather data for match {match_id} already exists, skipping"
                    )
                    continue

                match_datetime_str = match["utcDate"]
                home_team = match["homeTeam"]
                coords_str = home_team.get("address", "")

                if not coords_str:
                    logging.warning(f"No coordinates found for match {match_id}")
                    error_count += 1
                    continue

                try:
                    lat, lon = map(lambda x: float(x.strip()), coords_str.split(","))
                except ValueError as e:
                    logging.warning(
                        f"Invalid coordinates format '{coords_str}' for match {match_id}: {e}"
                    )
                    error_count += 1
                    continue

                match_datetime = datetime.fromisoformat(
                    match_datetime_str.replace("Z", "+00:00")
                )

                weather_data = fetch_weather_by_coordinates(lat, lon, match_datetime)

                if weather_data:
                    try:
                        validate_single(weather_data, WeatherContract)
                    except Exception as ve:
                        validation_error_count += 1
                        logging.warning(f"Weather validation failed for match {match_id}: {ve}")
                        continue
                    if save_weather_to_gcs(weather_data, match_id):
                        processed_count += 1
                        processed_weather_data.append(weather_data)
                else:
                    logging.warning(f"No weather data fetched for match {match_id}")
                    error_count += 1

            except Exception as e:
                error_count += 1
                logging.error(f"Error processing match {match_id}: {e}")

        if validation_error_count > 0:
            validation_message = f"{validation_error_count} weather records failed Pydantic validation and were skipped"
            logging.warning(validation_message)
            send_discord_notification(
                "⚠️ Weather Data: Validation Issues", validation_message, 16776960
            )

        if processed_count > 0:
            success_message = (
                f"🌤️ Successfully saved weather data for {processed_count} new matches"
            )
            logging.info(success_message)
            send_discord_notification("Weather Data Update", success_message, 65280)

        else:
            message = "📝 No new weather data needed to be saved"
            logging.info(message)
            send_discord_notification("Weather Data Update", message, 16776960)

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            GCP_PROJECT_ID, "convert_weather_to_parquet_topic"
        )

        publish_data = {
            "action": "convert_weather",
            "weather_data": processed_weather_data,
            "stats": {
                "processed_count": processed_count,
                "error_count": error_count,
                "timestamp": datetime.now().isoformat(),
            },
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to convert-weather-to-parquet-topic with ID: {publish_result}"
        )

        return "Process completed.", 200

    except Exception as e:
        error_message = f"❌ Weather data update failed: {str(e)}"
        send_discord_notification("Weather Data Update", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500


def get_match_data():
    """
    Fetch match data from your existing matches table in BigQuery.
    """
    client = bigquery.Client()
    # nosec - Safe in BigQuery context
    query = load_query("matches_with_teams").format(project_id=client.project)  # nosec B608
    query_job = client.query(query)
    results = query_job.result()
    matches = []
    for row in results:
        matches.append(
            {
                "id": row.match_id,
                "utcDate": row.utcDate.isoformat(),
                "homeTeam": {
                    "id": row.home_team_id,
                    "name": row.home_team_name,
                    "address": row.home_team_address,
                },
                "competition": {
                    "code": row.competition_code,
                    "name": row.competition_name,
                },
            }
        )
    return matches
