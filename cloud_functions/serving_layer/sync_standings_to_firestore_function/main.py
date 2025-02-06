import base64
import json
import os
import logging
import requests
from datetime import datetime
from google.cloud import bigquery, firestore


def sync_standings_to_firestore(event, context):
    """Cloud Function to sync current standings from BigQuery to Firestore."""
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

        bq_client = bigquery.Client()
        db = firestore.Client()

        LEAGUE_IDS = [2002, 2014, 2015, 2019, 2021]  # BL1, SA, FL1, SA, PL

        query = """
        WITH LatestStandings AS (
            SELECT 
                competitionId,
                standings,
                fetchDate,
                ROW_NUMBER() OVER(PARTITION BY competitionId ORDER BY fetchDate DESC) as rn
            FROM `sports_data_eu.standings_processed`
            WHERE competitionId IN UNNEST(@league_ids)
        )
        SELECT 
            competitionId,
            standings
        FROM LatestStandings
        WHERE rn = 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("league_ids", "INT64", LEAGUE_IDS),
            ]
        )

        query_job = bq_client.query(query, job_config=job_config)

        standings_collection = db.collection("current_standings")
        sync_count = 0

        for row in query_job:
            standings_doc = standings_collection.document(str(row.competitionId))
            standings_data = {
                "competition_id": row.competitionId,
                "table": [
                    {
                        "position": team["position"],
                        "team_id": team["teamId"],
                        "team_name": team["teamName"],
                        "team_short_name": team["teamShortName"],
                        "team_tla": team["teamTLA"],
                        "team_crest": team["teamCrest"],
                        "played_games": team["playedGames"],
                        "points": team["points"],
                        "won": team["won"],
                        "draw": team["draw"],
                        "lost": team["lost"],
                        "goals_for": team["goalsFor"],
                        "goals_against": team["goalsAgainst"],
                        "goal_difference": team["goalDifference"],
                        "form": team["form"],
                    }
                    for team in row.standings
                ],
                "last_updated": datetime.now().isoformat(),
            }

            standings_doc.set(standings_data, merge=False)
            sync_count += 1

        status_message = (
            f"Successfully synced standings for {sync_count} competitions to Firestore"
        )
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
