import json
import os
import logging
from datetime import datetime
from pathlib import Path
from google.cloud import firestore, bigquery, pubsub_v1
import base64
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def load_query(name: str) -> str:
    """Load SQL query from file."""
    sql_path = Path(__file__).parent / "sql" / f"{name}.sql"
    return sql_path.read_text()


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

        query = load_query("latest_standings")

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

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "sync_summaries_to_firestore_topic"
        )
        publish_data = {
            "action": "sync_summaries_to_firestore",
            "timestamp": datetime.now().isoformat(),
        }
        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )
        publish_result = future.result()
        logging.info(
            f"Published message to sync-summaries-to-firestore-topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Standings Firestore sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Standings Firestore Sync: Failure", error_message, 16711680
        )
        return error_message, 500
