import logging
from typing import List, Dict, Any

from .utils.api_helpers_league import get_league_data
from .utils.bigquery_helpers_league import insert_data_into_bigquery

from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)
from cloud_functions.data_contracts.league_contract import LeagueContract
from cloud_functions.data_contracts.validation import (
    validate_records,
    format_validation_summary,
)


def fetch_league_data(event, context):
    """
    Cloud Function to fetch leagues and teams data and load into BigQuery,
    runs only on odd weeks with Discord notifications.
    """

    try:
        league_codes = [
            "PL",
            "FL1",
            "BL1",
            "SA",
            "PD",
        ]  # Premier League, Ligue 1, Bundesliga, Serie A, La Liga
        league_data_list = []

        for code in league_codes:
            league_data = get_league_data(code)
            league_data_list.append(league_data)

        valid_leagues, validation_errors = validate_records(
            league_data_list, LeagueContract
        )

        if validation_errors:
            summary = format_validation_summary(
                len(league_data_list), len(valid_leagues), validation_errors
            )
            logging.warning(f"League data validation issues:\n{summary}")
            send_discord_notification(
                "⚠️ Fetch League Data: Validation Issues", summary, 16776960
            )

        load_data_into_bigquery(valid_leagues)

        send_discord_notification(
            "✅ Fetch League Data: Success",
            "League data fetched, loaded into BigQuery successfully.",
            65280,  # Green
        )
        return "League data processed and pipeline triggered successfully.", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        send_discord_notification(
            "❌ Fetch League Data: Failure",
            error_message,
            16711680,  # Red
        )
        raise


def load_data_into_bigquery(league_data_list: List[Dict[str, Any]]):
    league_table_name = "leagues"
    team_table_name = "teams"

    leagues = []
    teams = []

    for league_data in league_data_list:
        league_info = {
            "id": league_data["id"],
            "name": league_data["name"],
            "code": league_data["code"],
            "logo": league_data.get("emblem"),
            "current_season_start_date": league_data["currentSeason"]["startDate"],
            "current_season_end_date": league_data["currentSeason"]["endDate"],
            "current_season_matchday": league_data["currentSeason"].get(
                "currentMatchday"
            ),
        }
        leagues.append(league_info)

        if league_data.get("teams"):
            for team in league_data["teams"]:
                team_info = {
                    "id": team["id"],
                    "name": team["name"],
                    "tla": team.get("tla"),
                    "logo": team.get("crest"),
                    "venue": team.get("venue"),
                    "address": team.get("address"),
                    "league_id": league_data["id"],
                }
                teams.append(team_info)

    insert_data_into_bigquery(league_table_name, leagues)
    insert_data_into_bigquery(team_table_name, teams)
