import base64
import json
import os
import logging
from google.cloud import storage, pubsub_v1
import polars as pl
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def transform_to_parquet(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if (
            "action" not in message_data
            or message_data["action"] != "convert_standings"
        ):
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Convert Standings to Parquet: Invalid Trigger",
                error_message,
                16711680,
            )
            return error_message, 500

        bucket_name = os.environ.get("BUCKET_NAME")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix="standings_data/")
        json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

        if not json_files:
            message = "No standings JSON files found to convert"
            logging.info(message)
            send_discord_notification(
                "📝 Convert Standings to Parquet: No Files", message, 16776960
            )

            publisher = pubsub_v1.PublisherClient()
            bigquery_topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"], "standings_to_bigquery_topic"
            )

            bigquery_message = {"action": "load_standings_to_bigquery"}

            future = publisher.publish(
                bigquery_topic_path, data=json.dumps(bigquery_message).encode("utf-8")
            )

            publish_result = future.result()
            logging.info(
                f"Published no-files message to standings_to_bigquery_topic with ID: {publish_result}"
            )

            return message, 200

        processed_count = 0
        skipped_count = 0

        for json_file in json_files:
            parquet_name = json_file.replace(".json", ".parquet")
            parquet_path = f"standings_data_parquet/{os.path.basename(parquet_name)}"
            parquet_blob = bucket.blob(parquet_path)

            if parquet_blob.exists():
                skipped_count += 1
                logging.info(f"Parquet already exists for {json_file}")
                continue

            blob = bucket.blob(json_file)
            json_content = json.loads(blob.download_as_string())

            flattened_data = []
            for standing_type in json_content.get("standings", []):
                table_type = standing_type.get("type", "TOTAL")
                for team in standing_type.get("table", []):
                    team_data = {
                        "fetchDate": json_content["fetchDate"],
                        "competitionId": int(json_content["competitionId"]),
                        "season": json_content["season"],
                        "standingType": table_type,
                        **team,
                    }
                    flattened_data.append(team_data)

            df = pl.DataFrame(flattened_data)

            df.write_parquet("/tmp/temp.parquet")  # nosec B108
            parquet_blob.upload_from_filename("/tmp/temp.parquet")  # nosec B108
            processed_count += 1

        status_message = f"Processed {processed_count} standings files, skipped {skipped_count} existing files"
        logging.info(status_message)
        send_discord_notification(
            "✅ Convert Standings to Parquet: Complete", status_message, 65280
        )

        publisher = pubsub_v1.PublisherClient()
        bigquery_topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "standings_to_bigquery_topic"
        )

        bigquery_message = {"action": "load_standings_to_bigquery"}

        future = publisher.publish(
            bigquery_topic_path, data=json.dumps(bigquery_message).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            "Published message to standings_to_bigquery_topic with conversion stats"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during standings parquet conversion: {str(e)}"
        send_discord_notification(
            "❌ Convert Standings to Parquet: Failure", error_message, 16711680
        )
        logging.exception(error_message)
        return error_message, 500
