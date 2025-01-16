import pytest
from unittest.mock import patch
from cloud_functions.league_data.fetch_league_data_function.main import (
    fetch_league_data,
    load_data_into_bigquery,
)


@pytest.fixture
def sample_league_data():
    return {
        "id": 2021,
        "name": "Premier League",
        "code": "PL",
        "emblem": "https://example.com/logo.png",
        "currentSeason": {
            "startDate": "2023-08-12",
            "endDate": "2024-05-19",
            "currentMatchday": 5,
        },
        "teams": [
            {
                "id": 64,
                "name": "Liverpool FC",
                "tla": "LIV",
                "crest": "https://example.com/crest.png",
                "venue": "Anfield",
                "address": "Anfield Road Liverpool L4 0TH",
            }
        ],
    }


def test_fetch_league_data_success(sample_league_data):
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.league_data.fetch_league_data_function.main.get_league_data"
        ) as mock_get_league_data,
        patch(
            "cloud_functions.league_data.fetch_league_data_function.main.load_data_into_bigquery"
        ) as mock_load_data,
    ):
        mock_get_league_data.return_value = sample_league_data

        result, status_code = fetch_league_data(event, context)

        assert status_code == 200
        assert result == "League data processed and pipeline triggered successfully."
        mock_get_league_data.assert_called()
        mock_load_data.assert_called_once()


def test_load_data_into_bigquery(sample_league_data):
    with patch(
        "cloud_functions.league_data.fetch_league_data_function.main.insert_data_into_bigquery"
    ) as mock_insert:
        league_data_list = [sample_league_data]
        load_data_into_bigquery(league_data_list)

        assert mock_insert.call_count == 2

        leagues_expected = [
            {
                "id": sample_league_data["id"],
                "name": sample_league_data["name"],
                "code": sample_league_data["code"],
                "logo": sample_league_data.get("emblem"),
                "current_season_start_date": sample_league_data["currentSeason"][
                    "startDate"
                ],
                "current_season_end_date": sample_league_data["currentSeason"][
                    "endDate"
                ],
                "current_season_matchday": sample_league_data["currentSeason"].get(
                    "currentMatchday"
                ),
            }
        ]
        teams_expected = [
            {
                "id": team["id"],
                "name": team["name"],
                "tla": team.get("tla"),
                "logo": team.get("crest"),
                "venue": team.get("venue"),
                "address": team.get("address"),
                "league_id": sample_league_data["id"],
            }
            for team in sample_league_data["teams"]
        ]

        mock_insert.assert_any_call("leagues", leagues_expected)
        mock_insert.assert_any_call("teams", teams_expected)


def test_fetch_league_data_exception():
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.league_data.fetch_league_data_function.main.get_league_data"
        ) as mock_get_league_data,
        patch(
            "cloud_functions.league_data.fetch_league_data_function.main.load_data_into_bigquery"
        ),
    ):
        mock_get_league_data.side_effect = Exception("API error")

        with pytest.raises(Exception) as excinfo:
            fetch_league_data(event, context)

        assert "API error" in str(excinfo.value)
