import os
import requests
import logging
from typing import Dict, Any, Optional
from google.cloud import bigquery
import time

logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("API_FOOTBALL_KEY")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

if not API_KEY:
    raise ValueError("API_FOOTBALL_KEY is not set in environment variables.")
if not GOOGLE_MAPS_API_KEY:
    raise ValueError("GOOGLE_MAPS_API_KEY is not set in environment variables.")
if not GCP_PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID is not set in environment variables.")

BASE_URL = "https://api.football-data.org/v4"
HEADERS = {"X-Auth-Token": API_KEY}


def get_existing_teams_from_bq(project_id: str, dataset: str) -> dict:
    """Fetch existing team IDs and addresses from BigQuery"""
    client = bigquery.Client(project=project_id)
    query = """
        SELECT id, address
        FROM `{}.{}.teams`
    """.format(project_id, dataset)  # nosec B608

    results = client.query(query).result()
    return {row.id: row.address for row in results}


def get_stadium_coordinates(
    venue: str, team_name: str, team_city: str, team_country: str
) -> Optional[str]:
    """
    Get stadium coordinates using Google Maps Geocoding API with enhanced search queries and fallbacks.
    """
    search_queries = [
        f"{venue}, {team_city}, {team_country}",
        f"{venue}, {team_country}",
        f"{team_name} Stadium, {team_city}, {team_country}",
        f"{team_name} Stadium, {team_country}",
        f"{venue}, {team_country} football stadium",
        f"{team_name}, {team_country} football stadium",
    ]

    for query in search_queries:
        params = {"address": query, "key": GOOGLE_MAPS_API_KEY}
        logging.info(f"Attempting to find coordinates with query: {query}")

        try:
            response = requests.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params=params,
                timeout=180,
            )
            response.raise_for_status()
            data = response.json()

            if data.get("results"):
                location = data["results"][0]["geometry"]["location"]
                logging.info(f"Successfully retrieved coordinates for query: {query}")
                return f"{location['lat']},{location['lng']}"
            else:
                logging.warning(f"No results found for query: {query}")
        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP error occurred for query: {query}: {http_err}")
        except Exception as e:
            logging.error(f"An error occurred for query: {query}: {e}")

    logging.error(f"All attempts failed to get coordinates for team: {team_name}")
    return None


def get_league_data(league_code: str) -> Dict[str, Any]:
    """
    Fetches league information and its teams from the football API, including stadium coordinates.
    """
    max_retries = 3
    retry_delay = 60

    for attempt in range(max_retries):
        try:
            league_url = f"{BASE_URL}/competitions/{league_code}"
            league_response = requests.get(league_url, headers=HEADERS, timeout=180)
            league_response.raise_for_status()
            league_data = league_response.json()

            time.sleep(3)

            teams_url = f"{BASE_URL}/competitions/{league_code}/teams"
            teams_response = requests.get(teams_url, headers=HEADERS, timeout=180)
            teams_response.raise_for_status()
            teams_data = teams_response.json()

            assert GCP_PROJECT_ID is not None

            existing_teams = get_existing_teams_from_bq(
                GCP_PROJECT_ID, "sports_data_eu"
            )

            for team in teams_data.get("teams", []):
                team_id = team.get("id")

                if team_id in existing_teams:
                    logging.info(
                        f"Team {team.get('name')} already exists, skipping coordinates lookup"
                    )
                    team["address"] = existing_teams[team_id]
                    continue

                team_name = team.get("name", "")
                stadium_name = team.get("venue", "") or team_name
                team_address = team.get("address", "")
                team_area = team.get("area", {})
                team_city = ""
                team_country = ""

                if team_address and "," in team_address:
                    address_parts = [part.strip() for part in team_address.split(",")]
                    if len(address_parts) >= 2:
                        team_city = address_parts[-2]
                        team_country = address_parts[-1]
                else:
                    team_city = team_area.get("name", "")
                    team_country = league_data.get("area", {}).get("name", "")

                team_city = team_city if team_city else ""
                team_country = team_country if team_country else ""

                coordinates = get_stadium_coordinates(
                    stadium_name, team_name, team_city, team_country
                )
                team["address"] = coordinates

                if coordinates:
                    logging.info(f"Added coordinates for new team: {team_name}")
                else:
                    logging.warning(
                        f"Could not get coordinates for new team: {team_name}"
                    )

            league_data["teams"] = teams_data.get("teams", [])
            logging.info(f"Successfully fetched data for league code: {league_code}")

            return league_data

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                if attempt < max_retries - 1:
                    logging.warning(f"Rate limit hit, waiting {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
            raise

    raise Exception(f"Failed to fetch league data after {max_retries} attempts")
