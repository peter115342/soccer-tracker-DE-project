import base64
import json
import os
import logging
import requests
from datetime import datetime
from google.cloud import firestore, pubsub_v1


def sync_standings_to_firestore(event, context):
    """Cloud Function to sync current standings from football-data.org API to Firestore."""
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

        LEAGUE_IDS = [2002, 2014, 2015, 2019, 2021]  # BL1, SA, FL1, SA, PL

        API_KEY = os.environ.get("API_FOOTBALL_KEY")
        headers = {"X-Auth-Token": API_KEY}
        base_url = "http://api.football-data.org/v4/competitions"

        sync_count = 0
        standings_collection = db.collection("current_standings")

        for competition_id in LEAGUE_IDS:
            url = f"{base_url}/{competition_id}/standings"
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logging.error(
                    f"Failed to fetch standings for competition {competition_id}: {response.status_code}"
                )
                continue

            standings_data = response.json()

            total_standings = None
            for standing in standings_data.get("standings", []):
                if standing.get("type") == "TOTAL":
                    total_standings = standing.get("table", [])
                    break

            if not total_standings:
                logging.error(
                    f"No total standings found for competition {competition_id}"
                )
                continue

            firestore_data = {
                "competition_id": competition_id,
                "table": [
                    {
                        "position": team["position"],
                        "team_id": team["team"]["id"],
                        "team_name": team["team"]["name"],
                        "team_short_name": team["team"]["shortName"],
                        "team_tla": team["team"]["tla"],
                        "team_crest": team["team"]["crest"],
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
                    for team in total_standings
                ],
                "last_updated": datetime.now().isoformat(),
            }

            # Save to Firestore
            standings_doc = standings_collection.document(str(competition_id))
            standings_doc.set(firestore_data, merge=False)
            sync_count += 1

        status_message = (
            f"Successfully synced standings for {sync_count} competitions to Firestore"
        )
        logging.info(status_message)
        send_discord_notification(
            "✅ Standings Firestore Sync: Success", status_message, 65280
        )

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            os.environ["GCP_PROJECT_ID"], "sync_upcoming_matches_to_firestore_topic"
        )

        publish_data = {
            "action": "sync_upcoming_matches_to_firestore",
            "timestamp": datetime.now().isoformat(),
        }

        future = publisher.publish(
            topic_path, data=json.dumps(publish_data).encode("utf-8")
        )

        publish_result = future.result()
        logging.info(
            f"Published message to sync-upcoming-matches-to-firestore-topic with ID: {publish_result}"
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
