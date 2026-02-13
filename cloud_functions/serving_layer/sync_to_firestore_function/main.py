import base64
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery, firestore, pubsub_v1
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def load_query(name: str) -> str:
    """Load SQL query from file."""
    sql_path = Path(__file__).parent / "sql" / f"{name}.sql"
    return sql_path.read_text()


def sync_matches_to_firestore(event, context):
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "sync_matches_to_firestore":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Match Firestore Sync: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        bq_client = bigquery.Client()
        db = firestore.Client()

        query = load_query("matches_with_details")

        query_job = bq_client.query(query)
        matches_collection = db.collection("matches")

        sync_count = 0
        for row in query_job:
            reddit_data = None
            if row.threads:
                reddit_data = {
                    "threads": [
                        {
                            "thread_type": thread.get("thread_type"),
                            "thread_id": thread.get("thread_id"),
                            "created_at": thread.get("created_at"),
                            "score": thread.get("score"),
                            "num_comments": thread.get("num_comments"),
                            "comments": [
                                {
                                    "id": c.get("id"),
                                    "body": c.get("body"),
                                    "score": c.get("score"),
                                    "author": c.get("author"),
                                    "created_at": c.get("created_at"),
                                }
                                for c in (thread.get("comments") or [])
                            ],
                        }
                        for thread in row.threads
                    ]
                }

            match_data = {
                "match_id": row.id,
                "date": row.utcDate.isoformat(),
                "status": row.status,
                "home_team": row.home_team,
                "home_team_id": row.home_team_id,
                "away_team": row.away_team,
                "away_team_id": row.away_team_id,
                "home_team_logo": row.home_team_logo,
                "away_team_logo": row.away_team_logo,
                "home_score": row.home_score,
                "away_score": row.away_score,
                "league": {
                    "id": row.league_id,
                    "name": row.league_name,
                    "logo": row.league_logo,
                },
                "weather": {
                    "apparent_temperature": row.apparent_temperature,
                    "temperature": row.temperature_2m,
                    "precipitation": row.precipitation,
                    "wind_speed": row.windspeed_10m,
                    "cloud_cover": row.cloudcover,
                    "humidity": row.relativehumidity_2m,
                    "weathercode": row.weathercode,
                },
                "venue": row.venue,
                "location": {"lat": row.lat, "lon": row.lon},
                "reddit_data": reddit_data,
                "last_updated": datetime.now().isoformat(),
            }

            match_ref = matches_collection.document(str(row.id))
            match_ref.set(match_data)
            sync_count += 1

        status_message = f"Successfully synced {sync_count} matches to Firestore at {datetime.now().isoformat()}"
        logging.info(status_message)
        send_discord_notification(
            "✅ Match Firestore Sync: Success", status_message, 65280
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "sync_standings_to_firestore_topic"
        )
        publish_data = {
            "action": "sync_standings_to_firestore",
            "timestamp": datetime.now().isoformat(),
        }
        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )
        publish_result = future.result()
        logging.info(
            f"Published message to sync-standings-to-firestore-topic with ID: {publish_result}"
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during Match Firestore sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Match Firestore Sync: Failure", error_message, 16711680
        )
        return error_message, 500
