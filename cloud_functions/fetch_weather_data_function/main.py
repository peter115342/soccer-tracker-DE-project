import logging
import os
import json
import requests
from utils.weather_data_helper import fetch_weather_by_coordinates, save_weather_to_gcs
from google.cloud import bigquery
from datetime import datetime

def fetch_weather_data(request):
    """Fetches and stores weather data for football match locations in GCS."""
    try:
        if request.method not in ["GET", "POST"]:
            return ("Method not allowed", 405)

        logging.basicConfig(level=logging.INFO)
        match_data = get_match_data()

        if not match_data:
            message = "📝 No matches to fetch weather data for"
            send_discord_notification("Weather Data Update", message, 16776960)
            return message, 200

        processed_count = 0
        error_count = 0

        for match in match_data:
            try:
                match_id = match["id"]
                match_datetime_str = match["utcDate"]
                home_team = match["homeTeam"]
                coords_str = home_team.get("address", "")

                if not coords_str:
                    logging.warning(f"No coordinates found for match {match_id}")
                    error_count += 1
                    continue

                try:
                    lat, lon = map(lambda x: float(x.strip()), coords_str.split(','))

                except ValueError as e:
                    logging.warning(f"Invalid coordinates format '{coords_str}' for match {match_id}: {e}")
                    error_count += 1
                    continue

                match_datetime = datetime.strptime(match_datetime_str, "%Y-%m-%dT%H:%M:%S%z")
                weather_data = fetch_weather_by_coordinates(lat, lon, match_datetime)

                if weather_data:
                    if save_weather_to_gcs(weather_data, match_id):
                        processed_count += 1
                else:
                    logging.warning(f"No weather data fetched for match {match_id}")
                    error_count += 1

            except Exception as e:
                error_count += 1
                logging.error(f"Error processing match {match_id}: {e}")

        if processed_count > 0:
            success_message = f"🌤️ Successfully saved weather data for {processed_count} new matches"
            send_discord_notification("Weather Data Update", success_message, 65280)
        else:
            message = "📝 No new weather data needed to be saved"
            send_discord_notification("Weather Data Update", message, 16776960)
            return message, 200

        return success_message, 200

    except Exception as e:
        error_message = f"❌ Weather data update failed: {str(e)}"
        send_discord_notification("Weather Data Update", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get['DISCORD_WEBHOOK_URL']
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return

    discord_data = {
        "content": None,
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": color
            }
        ]
    }

    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, data=json.dumps(discord_data), headers=headers)
    if response.status_code != 204:
        logging.error(f"Failed to send Discord notification: {response.status_code}, {response.text}")

def get_match_data():
    """
    Fetch match data from your existing matches table in BigQuery.
    """
    client = bigquery.Client()
    query = f"""
        SELECT
            m.id AS match_id,
            m.utcDate AS utcDate,
            m.competition.id AS competition_code,
            m.competition.name AS competition_name,
            m.homeTeam.id AS home_team_id,
            m.homeTeam.name AS home_team_name,
            t.address AS home_team_address
        FROM `{client.project}.sports_data.match_data` AS m
        JOIN `{client.project}.sports_data.teams` AS t
        ON m.homeTeam.id = t.id
    """
    query_job = client.query(query)
    results = query_job.result()
    matches = []
    for row in results:
        matches.append({
            'id': row.match_id,
            'utcDate': row.utcDate.isoformat(),
            'homeTeam': {
                'id': row.home_team_id,
                'name': row.home_team_name,
                'address': row.home_team_address
            },
            'competition': {
                'code': row.competition_code,
                'name': row.competition_name
            }
        })
    return matches
