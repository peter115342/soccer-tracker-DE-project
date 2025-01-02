import base64
import json
import os
import logging
import requests
from datetime import datetime
from google.cloud import bigquery, pubsub_v1


def load_reddit_to_bigquery(event, context):
    """Background Cloud Function to update BigQuery external table for Reddit Parquet files in GCS."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "load_reddit_to_bigquery":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Reddit BigQuery Load: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        bucket_name = os.environ.get("BUCKET_NAME")
        bigquery_client = bigquery.Client()

        dataset_ref = bigquery_client.dataset("sports_data_raw_parquet")

        try:
            bigquery_client.get_dataset(dataset_ref)
            logging.info("Dataset 'sports_data_raw_parquet' exists.")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "europe-central2"
            bigquery_client.create_dataset(dataset)
            logging.info("Created dataset 'sports_data_raw_parquet'.")

        table_ref = dataset_ref.table("reddit_parquet")

        schema = [
            bigquery.SchemaField("match_id", "STRING"),
            bigquery.SchemaField("thread_id", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("body", "STRING"),
            bigquery.SchemaField("created_utc", "FLOAT64"),
            bigquery.SchemaField("score", "INT64"),
            bigquery.SchemaField(
                "top_comments",
                "STRUCT",
                fields=[
                    bigquery.SchemaField(
                        "list",
                        "RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField(
                                "element",
                                "RECORD",
                                fields=[
                                    bigquery.SchemaField("id", "STRING"),
                                    bigquery.SchemaField("body", "STRING"),
                                    bigquery.SchemaField("score", "INT64"),
                                    bigquery.SchemaField("author", "STRING"),
                                ],
                            )
                        ],
                    )
                ],
            ),
        ]

        try:
            table = bigquery_client.get_table(table_ref)
            logging.info("Table 'reddit_parquet' exists.")

            external_config = bigquery.ExternalConfig("PARQUET")
            external_config.source_uris = [
                f"gs://{bucket_name}/reddit_data_parquet/*.parquet"
            ]
            external_config.schema = schema
            external_config.hive_partitioning_options = (
                bigquery.HivePartitioningOptions(
                    mode="CUSTOM",
                    source_uri_prefix=f"gs://{bucket_name}/reddit_data_parquet/",
                    fields=["match_id"],
                )
            )

            table.external_data_configuration = external_config
            bigquery_client.update_table(table, ["external_data_configuration"])
            logging.info("Updated external table 'reddit_parquet' configuration.")

        except Exception:
            external_config = bigquery.ExternalConfig("PARQUET")
            external_config.source_uris = [
                f"gs://{bucket_name}/reddit_data_parquet/*.parquet"
            ]
            external_config.schema = schema
            external_config.hive_partitioning_options = (
                bigquery.HivePartitioningOptions(
                    mode="CUSTOM",
                    source_uri_prefix=f"gs://{bucket_name}/reddit_data_parquet/",
                    fields=["match_id"],
                )
            )

            table = bigquery.Table(table_ref)
            table.external_data_configuration = external_config
            bigquery_client.create_table(table)
            logging.info("Created external table 'reddit_parquet'.")

        query = """
            SELECT COUNT(*) as reddit_count 
            FROM `sports_data_raw_parquet.reddit_parquet`
        """
        query_job = bigquery_client.query(query)
        reddit_count = next(query_job.result())[0]

        status_message = (
            f"External table 'reddit_parquet' has been updated.\n"
            f"Total reddit posts available: {reddit_count}"
        )

        logging.info(status_message)
        send_discord_notification(
            "✅ Reddit BigQuery External Table: Updated", status_message, 65280
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "transform_reddit_topic"
        )

        publish_data = {
            "action": "transform_reddit",
            "timestamp": datetime.now().isoformat(),
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to transform-reddit-topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Reddit BigQuery external table update: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Reddit BigQuery External Table: Failure", error_message, 16711680
        )
        return error_message, 500


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
