# import base64
# import json
# import os
# import logging
# import requests
# from datetime import datetime
# from google.cloud import storage, bigquery, pubsub_v1
# from utils.bigquery_helpers_parquet_match import (
#     load_match_parquet_to_bigquery,
# )

# def load_matches_to_bigquery(event, context):
#     """Background Cloud Function to load Match Parquet files from GCS to BigQuery."""
#     try:
#         pubsub_message = base64.b64decode(event['data']).decode('utf-8')
#         message_data = json.loads(pubsub_message)

#         if message_data.get('action') != 'load_matches_to_bigquery':
#             error_message = "Invalid message format"
#             logging.error(error_message)
#             send_discord_notification("❌ Match BigQuery Load: Invalid Trigger", error_message, 16711680)
#             return error_message, 500

#         bucket_name = os.environ.get('BUCKET_NAME')
#         storage_client = storage.Client()
#         bucket = storage_client.bucket(bucket_name)
#         bigquery_client = bigquery.Client()

#         job_config = bigquery.LoadJobConfig(
#             source_format=bigquery.SourceFormat.PARQUET,
#             schema_update_options=[
#                 bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
#             ],
#             write_disposition=bigquery.WriteDisposition.WRITE_APPEND
#         )

#         dataset_ref = bigquery_client.dataset('sports_data')
#         try:
#             bigquery_client.get_dataset(dataset_ref)
#         except Exception:
#             dataset = bigquery.Dataset(dataset_ref)
#             dataset.location = "US"
#             bigquery_client.create_dataset(dataset)

#         table_ref = dataset_ref.table('matches_parquet')
#         try:
#             bigquery_client.get_table(table_ref)
#         except Exception:
#             table = bigquery.Table(table_ref)
#             bigquery_client.create_table(table)

#         match_files = [blob.name for blob in bucket.list_blobs(prefix='match_data_parquet/')]
        
#         match_loaded, match_processed = load_match_parquet_to_bigquery(
#             bigquery_client,
#             'sports_data',
#             'matches_parquet',
#             bucket_name,
#             match_files,
#             job_config=job_config
#         )

#         status_message = (
#             f"Processed {len(match_files)} match files\n"
#             f"Successfully loaded: {match_loaded} matches\n"
#         )

#         logging.info(status_message)
#         send_discord_notification("✅ Match BigQuery Load: Complete", status_message, 65280)

#         publisher = pubsub_v1.PublisherClient()
#         topic_path = publisher.topic_path(os.environ['GCP_PROJECT_ID'], 'fetch_weather_data_topic')

#         publish_data = {
#             "action": "fetch_weather",
#             "timestamp": datetime.now().isoformat()
#         }

#         future = publisher.publish(
#             topic_path,
#             data=json.dumps(publish_data).encode('utf-8')
#         )

#         publish_result = future.result()
#         logging.info(f"Published message to fetch-weather-topic with ID: {publish_result}")

#         return status_message, 200

#     except Exception as e:
#         error_message = f"Error during Match BigQuery load: {str(e)}"
#         logging.exception(error_message)
#         send_discord_notification("❌ Match BigQuery Load: Failure", error_message, 16711680)
#         return error_message, 500

# def send_discord_notification(title: str, message: str, color: int):
#     """Sends a notification to Discord with the specified title, message, and color."""
#     webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
#     if not webhook_url:
#         logging.error("Discord webhook URL not configured")
#         return

#     if len(message) > 2000:
#         message = message[:1997] + "..."

#     discord_data = {
#         "content": None,
#         "embeds": [
#             {
#                 "title": title,
#                 "description": message,
#                 "color": color,
#             }
#         ]
#     }

#     try:
#         headers = {"Content-Type": "application/json"}
#         response = requests.post(
#             webhook_url, 
#             data=json.dumps(discord_data), 
#             headers=headers, 
#             timeout=10
#         )
#         response.raise_for_status()
#         if response.status_code != 204:
#             logging.error(f"Discord notification failed with status code: {response.status_code}")
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Discord notification failed: {str(e)}")
