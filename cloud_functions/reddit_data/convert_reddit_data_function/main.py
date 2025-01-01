import base64
import json
import os
import logging
import requests
from google.cloud import storage, pubsub_v1
import polars as pl


def transform_reddit_to_parquet(event, context):
    """Cloud Function to convert Reddit JSON data to Parquet format."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if "action" not in message_data or message_data["action"] != "convert_reddit":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Convert Reddit to Parquet: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        bucket_name = os.environ.get("BUCKET_NAME")
        if not bucket_name:
            error_message = "BUCKET_NAME environment variable not set."
            logging.error(error_message)
            send_discord_notification(
                "❌ Convert Reddit to Parquet: Configuration Error",
                error_message,
                16711680,
            )
            return error_message, 500

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix="reddit_data/")
        json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

        if not json_files:
            message = "No Reddit JSON files found to convert"
            logging.info(message)
            send_discord_notification(
                "📝 Convert Reddit to Parquet: No Files", message, 16776960
            )

            publisher = pubsub_v1.PublisherClient()
            next_topic_path = publisher.topic_path(
                os.environ["GCP_PROJECT_ID"],
                "reddit_to_bigquery_topic",
            )

            next_message = {"action": "load_reddit_to_bigquery"}

            future = publisher.publish(
                next_topic_path, data=json.dumps(next_message).encode("utf-8")
            )

            publish_result = future.result()
            logging.info(f"Published message to next_topic with ID: {publish_result}")

            return message, 200

        processed_count = 0
        skipped_count = 0

        for json_file in json_files:
            parquet_name = json_file.replace(".json", ".parquet")
            parquet_path = f"reddit_data_parquet/{os.path.basename(parquet_name)}"
            parquet_blob = bucket.blob(parquet_path)

            if parquet_blob.exists():
                skipped_count += 1
                logging.info(f"Parquet already exists for {json_file}")
                continue

            blob = bucket.blob(json_file)
            json_content = json.loads(blob.download_as_string())

            # Ensure json_content is a list
            if isinstance(json_content, dict):
                json_content = [json_content]
            elif not isinstance(json_content, list):
                logging.warning(f"Unexpected data format in {json_file}")
                continue

            # Convert UNIX timestamps to datetime if needed
            for item in json_content:
                if "created_utc" in item:
                    item["created_utc"] = pl.from_epoch(item["created_utc"], unit="s")
                if "top_comments" in item and isinstance(item["top_comments"], list):
                    for comment in item["top_comments"]:
                        if "created_utc" in comment:
                            comment["created_utc"] = pl.from_epoch(
                                comment["created_utc"], unit="s"
                            )

            df = pl.DataFrame(json_content)
            df = df.explode("top_comments")

            # Flatten the nested 'top_comments' dictionary
            if "top_comments" in df.columns:
                df = df.with_columns(
                    [
                        pl.col("top_comments")
                        .apply(lambda x: x.get("id"))
                        .alias("comment_id"),
                        pl.col("top_comments")
                        .apply(lambda x: x.get("body"))
                        .alias("comment_body"),
                        pl.col("top_comments")
                        .apply(lambda x: x.get("score"))
                        .alias("comment_score"),
                        pl.col("top_comments")
                        .apply(lambda x: x.get("author"))
                        .alias("comment_author"),
                        pl.col("top_comments")
                        .apply(lambda x: x.get("created_utc"))
                        .alias("comment_created_utc"),
                    ]
                )
                df = df.drop("top_comments")

            df.write_parquet("/tmp/temp.parquet")
            parquet_blob.upload_from_filename("/tmp/temp.parquet")
            processed_count += 1

        status_message = (
            f"Processed {processed_count} files, skipped {skipped_count} existing files"
        )
        logging.info(status_message)
        send_discord_notification(
            "✅ Convert Reddit to Parquet: Complete", status_message, 65280
        )

        # Publish message to the next Pub/Sub topic
        publisher = pubsub_v1.PublisherClient()
        next_topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "reddit_to_bigquery_topic"
        )

        next_message = {"action": "load_reddit_to_bigquery"}

        future = publisher.publish(
            next_topic_path, data=json.dumps(next_message).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to reddit_to_bigquery_topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Reddit data parquet conversion: {str(e)}"
        send_discord_notification(
            "❌ Convert Reddit to Parquet: Failure", error_message, 16711680
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
