import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.fetch_league_data_function.utils.api_helpers_league import (
    get_league_data,
    get_stadium_coordinates,
)


@pytest.fixture
def mock_api_response():
    return {
        "id": 1,
        "name": "Test League",
        "code": "TL",
        "area": {"name": "Test Country"},
        "currentSeason": {"startDate": "2023-01-01", "endDate": "2023-12-31"},
    }


@pytest.fixture
def mock_teams_response():
    return {
        "teams": [
            {
                "id": 1,
                "name": "Test Team",
                "venue": "Test Stadium",
                "address": "Test Street, Test City, Test Country",
            }
        ]
    }


def test_get_league_data_success():
    with (
        patch("requests.get") as mock_get,
        patch(
            "cloud_functions.fetch_league_data_function.utils.api_helpers_league.get_existing_teams_from_bq"
        ) as mock_bq,
        patch(
            "cloud_functions.fetch_league_data_function.utils.api_helpers_league.get_stadium_coordinates"
        ) as mock_coords,
    ):
        mock_get.side_effect = [
            MagicMock(json=lambda: mock_api_response()),
            MagicMock(json=lambda: mock_teams_response()),
        ]
        mock_bq.return_value = {}
        mock_coords.return_value = "51.123,-0.456"

        result = get_league_data("TL")

        assert result["name"] == "Test League"
        assert len(result["teams"]) == 1
        assert mock_get.call_count == 2


def test_get_stadium_coordinates_success():
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = {
            "results": [{"geometry": {"location": {"lat": 51.123, "lng": -0.456}}}]
        }

        result = get_stadium_coordinates(
            "Test Stadium", "Test Team", "Test City", "Test Country"
        )

        assert result == "51.123,-0.456"
