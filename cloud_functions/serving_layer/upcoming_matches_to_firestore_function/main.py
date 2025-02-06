import base64
import json
import os
import logging
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery, firestore


def sync_upcoming_matches_to_firestore(event, context):
    """Cloud Function to sync tomorrow's matches from BigQuery to Firestore."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "sync_upcoming_matches_to_firestore":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Upcoming Matches Firestore Sync: Invalid Trigger",
                error_message,
                16711680,
            )
            return error_message, 500

        bq_client = bigquery.Client()
        db = firestore.Client()

        tomorrow = (datetime.now() + timedelta(days=1)).date()

        query = """
        SELECT 
            m.id,
            m.utcDate,
            m.status,
            m.homeTeam.name as home_team,
            m.awayTeam.name as away_team,
            m.competition.id as competition_id,
            m.competition.name as competition_name
        FROM `sports_data_eu.matches_processed` m
        WHERE DATE(m.utcDate) = @tomorrow_date
        AND m.competition.id IN (2002, 2014, 2015, 2019, 2021)  -- Top 5 leagues
        ORDER BY m.utcDate
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tomorrow_date", "DATE", tomorrow),
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)

        upcoming_collection = db.collection("upcoming_matches")
        date_doc = upcoming_collection.document(tomorrow.isoformat())

        matches_data = {
            "date": tomorrow.isoformat(),
            "matches": [],
            "last_updated": datetime.now().isoformat(),
        }

        match_count = 0
        for row in query_job:
            match_data = {
                "match_id": row.id,
                "kickoff": row.utcDate.isoformat(),
                "status": row.status,
                "home_team": row.home_team,
                "away_team": row.away_team,
                "competition_id": row.competition_id,
                "competition_name": row.competition_name,
            }
            matches_data["matches"].append(match_data)
            match_count += 1

        date_doc.set(matches_data, merge=False)

        status_message = f"Successfully synced {match_count} upcoming matches for {tomorrow.isoformat()}"
        logging.info(status_message)
        send_discord_notification(
            "✅ Upcoming Matches Firestore Sync: Success", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Upcoming Matches Firestore sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Upcoming Matches Firestore Sync: Failure", error_message, 16711680
        )
        return error_message, 500


def send_discord_notification(title: str, message: str, color: int):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return
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
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        webhook_url, data=json.dumps(discord_data), headers=headers, timeout=90
    )
    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
