import base64
import json
import os
import logging
import requests
from google.cloud import dataplex_v1


def trigger_dataplex_scans(event, context):
    """Triggers Dataplex data quality scans for all tables
    Args:
         event (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event metadata.
    """
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if "action" not in message_data or message_data["action"] != "trigger_scans":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Dataplex Scans: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        client = dataplex_v1.DataQualityServiceClient()
        project_id = os.environ.get("GCP_PROJECT_ID")
        lake_id = os.environ.get("LAKE_ID")

        scan_ids = [
            "matches_processed_scan",
            "weather_processed_scan",
            "reddit_processed_scan",
            "standings_processed_scan",
        ]

        triggered_scans = []

        for scan_id in scan_ids:
            scan_name = f"projects/{project_id}/locations/europe-central2/lakes/{lake_id}/dataScanJobs/{scan_id}"

            request = dataplex_v1.TriggerScanRequest(name=scan_name)

            operation = client.trigger_scan(request=request)
            triggered_scans.append(operation.result())

        status_message = (
            f"Successfully triggered {len(triggered_scans)} Dataplex quality scans"
        )
        logging.info(status_message)
        send_discord_notification("✅ Dataplex Scans: Triggered", status_message, 65280)

        return status_message, 200

    except Exception as e:
        error_message = f"Error triggering Dataplex scans: {str(e)}"
        send_discord_notification("❌ Dataplex Scans: Failed", error_message, 16711680)
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
        webhook_url, data=json.dumps(discord_data), headers=headers, timeout=90
    )
    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
