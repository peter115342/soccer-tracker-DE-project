import base64
import json
import os
import logging
import requests
from datetime import datetime, timedelta
from google.cloud import firestore


def sync_upcoming_matches_to_firestore(event, context):
    """Cloud Function to sync tomorrow's matches from football-data API to Firestore."""
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

        db = firestore.Client()
        tomorrow = (datetime.now() + timedelta(days=1)).date()

        upcoming_collection = db.collection("upcoming_matches")
        docs = upcoming_collection.stream()
        for doc in docs:
            doc.reference.delete()

        api_key = os.environ.get("API_FOOTBALL_KEY")
        if not api_key:
            raise ValueError("Football Data API key not configured")

        headers = {"X-Auth-Token": api_key}

        competitions = [2002, 2014, 2015, 2019, 2021]  # BL1, SA, FL1, SA, PL
        matches_data = {
            "date": tomorrow.isoformat(),
            "matches": [],
            "last_updated": datetime.now().isoformat(),
        }

        for competition_id in competitions:
            url = (
                f"http://api.football-data.org/v4/competitions/{competition_id}/matches"
            )
            params = {"dateFrom": tomorrow.isoformat(), "dateTo": tomorrow.isoformat()}

            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for match in data.get("matches", []):
                    match_data = {
                        "match_id": match["id"],
                        "kickoff": match["utcDate"],
                        "status": match["status"],
                        "home_team": match["homeTeam"]["name"],
                        "away_team": match["awayTeam"]["name"],
                        "competition_id": match["competition"]["id"],
                        "competition_name": match["competition"]["name"],
                    }
                    matches_data["matches"].append(match_data)
            else:
                logging.warning(
                    f"Failed to fetch matches for competition {competition_id}: {response.status_code}"
                )

        upcoming_collection = db.collection("upcoming_matches")
        date_doc = upcoming_collection.document(tomorrow.isoformat())
        date_doc.set(matches_data, merge=False)

        match_count = len(matches_data["matches"])
        status_message = f"Successfully synced {match_count} upcoming matches for {tomorrow.isoformat()} and cleared old data"
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
