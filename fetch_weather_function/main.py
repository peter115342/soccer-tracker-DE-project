import logging
from typing import Union
import os
import json
import requests
from utils.api_helpers_weather import fetch_weather_by_coordinates
from utils.bigquery_helpers_weather import insert_weather_data_into_bigquery
from utils.geocoding_helper import get_coordinates
from google.cloud import bigquery
from datetime import datetime

def fetch_weather_data(request):
    """Fetches and stores weather data for football match locations."""
    try:
        if request.method not in ["GET", "POST"]:
            return ("Method not allowed", 405)

        logging.basicConfig(level=logging.INFO)
        table_name = "weather_data"
        match_data = get_match_data()

        if not match_data:
            message = "No match data found to process weather information."
            send_discord_notification("ℹ️ Weather Data: No Matches", message, 16776960)
            return message, 200

        weather_data_list = []
        processed_count = 0
        error_count = 0

        for match in match_data:
            try:
                match_id = match["id"]
                match_datetime_str = match["utcDate"]
                home_team = match["homeTeam"]
                address = home_team.get("address", "")

                if not address:
                    logging.warning(f"No address found for match {match_id}")
                    error_count += 1
                    continue

                competition = match.get("competition", {}).get("code", "")
                country_code = get_country_code_from_competition(competition)
                if not country_code:
                    logging.warning(f"No country code found for competition '{competition}'")
                    error_count += 1
                    continue

                coords = get_coordinates(address, country_code)
                if not coords:
                    logging.warning(f"Could not get coordinates for address '{address}' for match {match_id}")
                    error_count += 1
                    continue

                lat = coords["lat"]
                lon = coords["lon"]

                match_datetime = datetime.strptime(match_datetime_str, "%Y-%m-%dT%H:%M:%S%z")
                match_datetime.strftime("%Y-%m-%d")

                weather_data = fetch_weather_by_coordinates(lat, lon, match_datetime)

                if weather_data:
                    hourly_data = weather_data.get("hourly", {})
                    times = hourly_data.get("time", [])
                    try:
                        match_time_str = match_datetime.strftime('%Y-%m-%dT%H:%M')
                        if match_time_str in times:
                            index = times.index(match_time_str)
                        else:
                            index = min(
                                range(len(times)),
                                key=lambda i: abs(datetime.strptime(times[i], '%Y-%m-%dT%H:%M') - match_datetime)
                            )
                        weather_record = {
                            "match_id": match_id,
                            "lat": lat,
                            "lon": lon,
                            "timestamp": times[index],
                            "temperature_2m": hourly_data.get("temperature_2m", [None])[index],
                            "relativehumidity_2m": hourly_data.get("relativehumidity_2m", [None])[index],
                            "dewpoint_2m": hourly_data.get("dewpoint_2m", [None])[index],
                            "apparent_temperature": hourly_data.get("apparent_temperature", [None])[index],
                            "precipitation": hourly_data.get("precipitation", [None])[index],
                            "rain": hourly_data.get("rain", [None])[index],
                            "snowfall": hourly_data.get("snowfall", [None])[index],
                            "snow_depth": hourly_data.get("snow_depth", [None])[index],
                            "weathercode": hourly_data.get("weathercode", [None])[index],
                            "pressure_msl": hourly_data.get("pressure_msl", [None])[index],
                            "cloudcover": hourly_data.get("cloudcover", [None])[index],
                            "visibility": hourly_data.get("visibility", [None])[index],
                            "windspeed_10m": hourly_data.get("windspeed_10m", [None])[index],
                            "winddirection_10m": hourly_data.get("winddirection_10m", [None])[index],
                            "windgusts_10m": hourly_data.get("windgusts_10m", [None])[index],
                        }
                        weather_data_list.append(weather_record)
                        processed_count += 1
                    except Exception as e:
                        logging.error(f"Error processing weather data for match {match_id}: {e}")
                        error_count += 1
                        continue
                else:
                    logging.warning(f"No weather data fetched for match {match_id}")
                    error_count += 1
            except Exception as e:
                error_count += 1
                logging.error(f"Error processing match {match_id}: {e}")

        if weather_data_list:
            insert_weather_data_into_bigquery(table_name, weather_data_list)
            success_message = f"Successfully processed weather data for {processed_count} matches. Errors: {error_count}"
            send_discord_notification("✅ Weather Data: Success", success_message, 65280)
        else:
            message = "No weather data was collected for processing"
            send_discord_notification("⚠️ Weather Data: No Data", message, 16776960)
            return message, 200

        return success_message, 200

    except Exception as e:
        error_message = f"An error occurred while fetching weather data: {str(e)}"
        send_discord_notification("❌ Weather Data: Failure", error_message, 16711680)
        logging.exception(error_message)
        return error_message, 500

def send_discord_notification(title: str, message: str, color: int):
    """Sends a notification to Discord with the specified title, message, and color."""
    webhook_url = os.environ.get('DISCORD_WEBHOOK_URL')
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

def get_country_code_from_competition(competition_code: str) -> Union[str, None]:
    """
    Returns the country code given the competition code.
    """
    competition_countries: dict[int, str] = {
        2021: "GB",  # Premier League - United Kingdom
        2002: "DE",  # Bundesliga - Germany
        2015: "ES",   # La Liga - Spain
        2019: "IT",   # Serie A - Italy
        2014: "FR",  # Ligue 1 - France
    }
    return competition_countries.get(int(competition_code))
