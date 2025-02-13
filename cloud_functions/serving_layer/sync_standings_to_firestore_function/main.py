import json
import os
import logging
from datetime import datetime
from google.cloud import firestore, bigquery, pubsub_v1
import base64
import requests


def sync_standings_to_firestore(event, context):
    """Cloud Function to sync latest standings from BigQuery to Firestore."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "sync_standings_to_firestore":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Standings Firestore Sync: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        db = firestore.Client()
        bq_client = bigquery.Client()

        query = """
            WITH LatestDates AS (
                SELECT 
                    competitionId,
                    MAX(fetchDate) as latest_date
                FROM `sports_data_eu.standings_processed`
                GROUP BY competitionId
            )
            SELECT s.*
            FROM `sports_data_eu.standings_processed` s
            INNER JOIN LatestDates l
                ON s.competitionId = l.competitionId
                AND s.fetchDate = l.latest_date
            WHERE s.standingType = 'TOTAL'
        """

        query_job = bq_client.query(query)
        results = query_job.result()

        sync_count = 0
        standings_collection = db.collection("current_standings")

        for row in results:
            firestore_data = {
                "competition_id": row.competitionId,
                "table": [
                    {
                        "position": standing["position"],
                        "team_id": standing["teamId"],
                        "team_name": standing["teamName"],
                        "team_short_name": standing["teamShortName"],
                        "team_tla": standing["teamTLA"],
                        "team_crest": standing["teamCrest"],
                        "played_games": standing["playedGames"],
                        "points": standing["points"],
                        "won": standing["won"],
                        "draw": standing["draw"],
                        "lost": standing["lost"],
                        "goals_for": standing["goalsFor"],
                        "goals_against": standing["goalsAgainst"],
                        "goal_difference": standing["goalDifference"],
                    }
                    for standing in row.standings
                ],
                "last_updated": datetime.now().isoformat(),
                "fetch_date": row.fetchDate.isoformat(),
                "season_id": row.seasonId,
                "current_matchday": row.currentMatchday,
            }

            standings_doc = standings_collection.document(str(row.competitionId))
            standings_doc.set(firestore_data, merge=False)
            sync_count += 1

        status_message = f"Successfully synced latest standings for {sync_count} competitions to Firestore"
        logging.info(status_message)
        send_discord_notification(
            "✅ Standings Firestore Sync: Success", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Standings Firestore sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Standings Firestore Sync: Failure", error_message, 16711680
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
