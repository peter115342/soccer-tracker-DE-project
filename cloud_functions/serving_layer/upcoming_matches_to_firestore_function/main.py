import base64
import json
import os
import logging
import requests
from datetime import datetime, timedelta
from google.cloud import firestore
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def sync_upcoming_matches_to_firestore(event, context):
    """Cloud Function to sync matches from the football-data API to Firestore.
    Handles both today's and tomorrow's matches based on the trigger action."""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        today = datetime.now().date()
        tomorrow = today + timedelta(days=1)

        if message_data.get("action") == "sync_today_matches":
            target_date = today
            sync_type = "Today's"
        elif message_data.get("action") == "sync_tomorrow_matches":
            target_date = tomorrow
            sync_type = "Tomorrow's"
        else:
            error_message = "Invalid action specified"
            logging.error(error_message)
            send_discord_notification(
                "❌ Upcoming Matches Sync: Invalid Trigger",
                error_message,
                16711680,
            )
            return error_message, 500

        db = firestore.Client()
        api_key = os.environ.get("API_FOOTBALL_KEY")
        if not api_key:
            raise ValueError("Football Data API key not configured")

        headers = {"X-Auth-Token": api_key}

        collection = db.collection("upcoming_matches")

        # Clear existing collection
        existing_docs = collection.stream()
        for doc in existing_docs:
            doc.reference.delete()

        competitions = [2002, 2014, 2015, 2019, 2021]  # BL1, SA, FL1, SA, PL
        matches_data = {
            "date": target_date.isoformat(),
            "matches": [],
            "last_updated": datetime.now().isoformat(),
        }

        for competition_id in competitions:
            url = (
                f"http://api.football-data.org/v4/competitions/{competition_id}/matches"
            )
            params = {
                "dateFrom": target_date.isoformat(),
                "dateTo": target_date.isoformat(),
            }

            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                for match in data.get("matches", []):
                    match_data = {
                        "match_id": match["id"],
                        "kickoff": match["utcDate"],
                        "status": match["status"],
                        "home_team": match["homeTeam"]["name"],
                        "home_team_crest": match["homeTeam"]["crest"],
                        "away_team": match["awayTeam"]["name"],
                        "away_team_crest": match["awayTeam"]["crest"],
                        "competition_id": match["competition"]["id"],
                        "competition_name": match["competition"]["name"],
                        "competition_emblem": match["competition"]["emblem"],
                        "area_name": match["area"]["name"],
                        "area_flag": match["area"]["flag"],
                    }
                    matches_data["matches"].append(match_data)
            else:
                logging.warning(
                    f"Failed to fetch matches for competition {competition_id}: {response.status_code}"
                )

        date_doc = collection.document(target_date.isoformat())
        date_doc.set(matches_data, merge=False)

        match_count = len(matches_data["matches"])
        status_message = f"Successfully synced {match_count} {sync_type} matches for {target_date.isoformat()}"
        logging.info(status_message)
        send_discord_notification(
            "✅ Upcoming Matches Sync: Success", status_message, 65280
        )

        return status_message, 200

    except Exception as e:
        error_message = f"Error during upcoming matches sync: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Upcoming Matches Sync: Failure", error_message, 16711680
        )
        return error_message, 500
