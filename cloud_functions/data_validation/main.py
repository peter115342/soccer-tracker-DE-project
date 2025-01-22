import base64
import json
import os
import logging
import requests
from google.cloud import dataplex_v1
from google.cloud import bigquery
import plotly.graph_objects as go


def get_table_record_counts() -> dict:
    """Fetches daily record counts from BigQuery tables."""
    client = bigquery.Client()

    query = """
    WITH matches_counts AS (
        SELECT DATE(utcDate) as date, COUNT(*) as count
        FROM `sports_data_eu.matches_processed`
        WHERE utcDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 DAY)
        GROUP BY date
        ORDER BY date
    ),
    weather_counts AS (
        SELECT DATE(timestamp) as date, COUNT(*) as count
        FROM `sports_data_eu.weather_processed`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 DAY)
        GROUP BY date
        ORDER BY date
    ),
    reddit_counts AS (
        SELECT match_date as date, COUNT(*) as count
        FROM `sports_data_eu.reddit_processed`
        WHERE match_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 DAY)
        GROUP BY date
        ORDER BY date
    )
    SELECT 
        COALESCE(m.date, w.date, r.date) as date,
        m.count as matches_count,
        w.count as weather_count,
        r.count as reddit_count
    FROM matches_counts m
    FULL OUTER JOIN weather_counts w ON m.date = w.date
    FULL OUTER JOIN reddit_counts r ON m.date = r.date
    ORDER BY date
    """  # nosec B608

    results = client.query(query).result()

    dates = []
    matches_counts = []
    weather_counts = []
    reddit_counts = []

    for row in results:
        dates.append(row.date)
        matches_counts.append(row.matches_count or 0)
        weather_counts.append(row.weather_count or 0)
        reddit_counts.append(row.reddit_count or 0)

    return {
        "dates": dates,
        "matches": matches_counts,
        "weather": weather_counts,
        "reddit": reddit_counts,
    }


def get_table_total_counts() -> dict:
    """Fetches cumulative record counts from BigQuery tables over the last 20 days."""
    client = bigquery.Client()

    query = """
    WITH matches_counts AS (
        SELECT
            DATE(utcDate) AS date,
            COUNT(*) AS daily_count
        FROM `sports_data_eu.matches_processed`
        WHERE utcDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 DAY)
        GROUP BY date
    ),
    matches_running_total AS (
        SELECT
            date,
            SUM(daily_count) OVER (ORDER BY date) AS count
        FROM matches_counts
    ),
    weather_counts AS (
        SELECT
            DATE(timestamp) AS date,
            COUNT(*) AS daily_count
        FROM `sports_data_eu.weather_processed`
        WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 DAY)
        GROUP BY date
    ),
    weather_running_total AS (
        SELECT
            date,
            SUM(daily_count) OVER (ORDER BY date) AS count
        FROM weather_counts
    ),
    reddit_counts AS (
        SELECT
            match_date AS date,
            COUNT(*) AS daily_count
        FROM `sports_data_eu.reddit_processed`
        WHERE match_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 20 DAY)
        GROUP BY date
    ),
    reddit_running_total AS (
        SELECT
            date,
            SUM(daily_count) OVER (ORDER BY date) AS count
        FROM reddit_counts
    )
    SELECT
        COALESCE(m.date, w.date, r.date) AS date,
        m.count AS matches_count,
        w.count AS weather_count,
        r.count AS reddit_count
    FROM matches_running_total m
    FULL OUTER JOIN weather_running_total w ON m.date = w.date
    FULL OUTER JOIN reddit_running_total r ON m.date = r.date
    ORDER BY date
    """  # nosec B608

    results = client.query(query).result()

    dates = []
    matches_counts = []
    weather_counts = []
    reddit_counts = []

    for row in results:
        dates.append(row.date)
        matches_counts.append(row.matches_count or 0)
        weather_counts.append(row.weather_count or 0)
        reddit_counts.append(row.reddit_count or 0)

    return {
        "dates": dates,
        "matches": matches_counts,
        "weather": weather_counts,
        "reddit": reddit_counts,
    }


def get_scan_results(project_id: str, table_suffix: str) -> dict:
    """Fetches scan results from BigQuery for a specific table."""
    client = bigquery.Client()

    query = f"""
    WITH recent_scans AS (
        SELECT job_start_time, rule_rows_passed_percent, rule_rows_evaluated
        FROM `{project_id}.processed_data_zone.{table_suffix}_processed_quality`
        WHERE job_start_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
    )
    SELECT
        AVG(rule_rows_passed_percent) as avg_pass_rate,
        SUM(rule_rows_evaluated) as total_rows_evaluated
    FROM recent_scans
    WHERE job_start_time = (SELECT MAX(job_start_time) FROM recent_scans)
    """  # nosec B608

    results = client.query(query).result()

    for row in results:
        return {
            "pass_rate": row.avg_pass_rate if row.avg_pass_rate else 0,
            "rows_evaluated": row.total_rows_evaluated
            if row.total_rows_evaluated
            else 0,
        }

    return {"pass_rate": 0, "rows_evaluated": 0}


def create_records_plot(record_counts: dict) -> bytes:
    """Creates a line plot of record counts over time."""
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=record_counts["dates"],
            y=record_counts["matches"],
            name="Matches/Weather",
            mode="lines+markers",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=record_counts["dates"],
            y=record_counts["reddit"],
            name="Reddit",
            mode="lines+markers",
        )
    )

    fig.update_layout(
        title="Daily Record Counts - Last 20 Days",
        xaxis_title="Date",
        yaxis_title="Number of Records",
        height=600,
        width=1000,
        showlegend=True,
    )

    img_bytes = fig.to_image(format="png")
    return img_bytes


def create_total_records_plot(record_counts: dict) -> bytes:
    """Creates a line plot of total record counts over time."""
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=record_counts["dates"],
            y=record_counts["matches"],
            name="Matches/Weather",
            mode="lines+markers",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=record_counts["dates"],
            y=record_counts["reddit"],
            name="Reddit",
            mode="lines+markers",
        )
    )

    fig.update_layout(
        title="Total Records Over Time - Last 20 Days",
        xaxis_title="Date",
        yaxis_title="Total Number of Records",
        height=600,
        width=1000,
        showlegend=True,
    )

    img_bytes = fig.to_image(format="png")
    return img_bytes


def send_discord_notification(
    title: str,
    message: str,
    color: int,
    daily_plot: bytes | None = None,
    total_plot: bytes | None = None,
):
    """Sends a notification to Discord with the specified title, message, color and optional plots."""
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return

    files = {}
    if daily_plot:
        files["daily_counts.png"] = ("daily_plot.png", daily_plot, "image/png")
    if total_plot:
        files["total_counts.png"] = ("total_plot.png", total_plot, "image/png")

    payload = {"embeds": [{"title": title, "description": message, "color": color}]}

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

        import time

        time.sleep(30)

        tables = ["matches", "weather", "reddit"]
        scan_results = {}
        for table in tables:
            scan_results[table] = get_scan_results(project_id, table)

        record_counts = get_table_record_counts()
        total_counts = get_table_total_counts()

        daily_plot = create_records_plot(record_counts)
        total_plot = create_total_records_plot(total_counts)

        scan_summary = "\n\n**Recent Scan Results:**"
        for table, results in scan_results.items():
            scan_summary += f"\n\n**{table}**:\n"
            scan_summary += f"Pass Rate: {results['pass_rate']:.2f}%"

        status_message = (
            f"Successfully triggered {len(triggered_scans)} Dataplex quality scans\n"
            f"Record count trends for the last 20 days shown in the graphs below."
            f"{scan_summary}"
        )

        logging.info(status_message)
        send_discord_notification(
            "✅ Dataplex Scans: Triggered",
            status_message,
            65280,
            daily_plot,
            total_plot,
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error triggering Dataplex scans: {str(e)}"
        send_discord_notification("❌ Dataplex Scans: Failed", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500
