import base64
import json
import os
import logging
from google.cloud import bigquery
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def load_weather_to_bigquery(event, context):
    """Background Cloud Function to update BigQuery external table for Weather Parquet files in GCS."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "load_weather_to_bigquery":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Weather BigQuery Load: Invalid Trigger", error_message, 16711680
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

        table_ref = dataset_ref.table("weather_parquet")

        try:
            table = bigquery_client.get_table(table_ref)
            logging.info("Table 'weather_parquet' exists.")

            external_config = bigquery.ExternalConfig("PARQUET")
            external_config.source_uris = [
                f"gs://{bucket_name}/weather_data_parquet/*.parquet"
            ]
            table.external_data_configuration = external_config
            bigquery_client.update_table(table, ["external_data_configuration"])
            logging.info("Updated external table 'weather_parquet' configuration.")

        except Exception:
            external_config = bigquery.ExternalConfig("PARQUET")
            external_config.source_uris = [
                f"gs://{bucket_name}/weather_data_parquet/*.parquet"
            ]
            table = bigquery.Table(table_ref)
            table.external_data_configuration = external_config
            bigquery_client.create_table(table)
            logging.info("Created external table 'weather_parquet'.")

        query = """
            SELECT COUNT(*) as weather_count 
            FROM `sports_data_raw_parquet.weather_parquet`
        """
        query_job = bigquery_client.query(query)
        weather_count = next(query_job.result())[0]

        status_message = (
            f"External table 'weather_parquet' has been updated.\n"
            f"Total weather records available: {weather_count}"
        )

        logging.info(status_message)
        send_discord_notification(
            "✅ Weather BigQuery External Table: Updated", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Weather BigQuery external table update: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Weather BigQuery External Table: Failure", error_message, 16711680
        )
        return error_message, 500
