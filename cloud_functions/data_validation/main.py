import base64
import json
import os
import logging
import requests
from google.cloud import dataplex_v1
from google.cloud import bigquery
import matplotlib.pyplot as plt
import io


def get_scan_results(table_prefix: str) -> dict:
    """Fetches scan results from BigQuery for a specific table."""
    client = bigquery.Client()

    query = f"""
    SELECT 
        CAST(job_start_time AS DATE) as scan_date,
        AVG(rule_rows_passed_percent) as avg_pass_rate,
        SUM(rule_rows_evaluated) as total_rows_evaluated
    FROM `processed_data_zone.{table_prefix}_processed_quality`
    WHERE job_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    GROUP BY scan_date
    ORDER BY scan_date
    """  # nosec B608

    results = client.query(query).result()

    dates = []
    pass_rates = []
    rows_evaluated = []

    for row in results:
        dates.append(row.scan_date)
        pass_rates.append(row.avg_pass_rate)
        rows_evaluated.append(row.total_rows_evaluated)

    return {"dates": dates, "pass_rates": pass_rates, "rows_evaluated": rows_evaluated}


def create_quality_plot(scan_results: dict) -> bytes:
    """Creates a plot of scan results over time."""
    plt.figure(figsize=(10, 6))

    plt.plot(
        scan_results["dates"],
        scan_results["pass_rates"],
        marker="o",
        label="Pass Rate %",
    )

    plt.title("Data Quality Scan Results - Last 7 Days")
    plt.xlabel("Date")
    plt.ylabel("Pass Rate %")
    plt.grid(True)
    plt.legend()

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    return buf.getvalue()


def send_discord_notification(title: str, message: str, color: int, scan_results=None):
    """Sends a notification to Discord with scan results and plot."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return

    files = {}
    embeds = [{"title": title, "description": message, "color": color}]

    if scan_results:
        for table, results in scan_results.items():
            if results["pass_rates"]:
                latest_rate = results["pass_rates"][-1]
                latest_rows = results["rows_evaluated"][-1]

                summary = (
                    "\n**{}**\nLatest Pass Rate: {:.2f}%\nRows Evaluated: {:,}".format(
                        table, latest_rate, latest_rows
                    )
                )
                embeds[0]["description"] = f"{embeds[0]['description']}{summary}"

        for table, results in scan_results.items():
            plot_data = create_quality_plot(results)
            files[f"{table}_plot.png"] = ("plot.png", plot_data, "image/png")

    payload = {"embeds": embeds}

    if files:
        response = requests.post(
            webhook_url,
            data={"payload_json": json.dumps(payload)},
            files=files,
            timeout=90,
        )
    else:
        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=90,
        )

    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )


def trigger_dataplex_scans(event, context):
    """Triggers Dataplex data quality scans for all tables and reports historical results
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

        client = dataplex_v1.DataScanServiceClient()
        project_id = os.environ.get("GCP_PROJECT_ID")
        location = os.environ.get("LOCATION", "europe-central2")

        scan_ids = [
            "matches-processed-scan",
            "weather-processed-scan",
            "reddit-processed-scan",
            "standings-processed-scan",
        ]

        triggered_scans = []

        for scan_id in scan_ids:
            scan_name = (
                f"projects/{project_id}/locations/{location}/dataScans/{scan_id}"
            )

            request = dataplex_v1.RunDataScanRequest(name=scan_name)
            operation = client.run_data_scan(request=request)
            triggered_scans.append(operation)

        tables = ["matches", "weather", "reddit", "standings"]
        scan_results = {}

        for table in tables:
            scan_results[table] = get_scan_results(table)

        status_message = (
            f"Successfully triggered {len(triggered_scans)} Dataplex quality scans\n"
            f"Historical results for the last 7 days attached below."
        )

        logging.info(status_message)
        send_discord_notification(
            "✅ Dataplex Scans: Triggered", status_message, 65280, scan_results
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error triggering Dataplex scans: {str(e)}"
        send_discord_notification("❌ Dataplex Scans: Failed", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500
