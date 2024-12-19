import pytest
from unittest.mock import patch
from cloud_functions.fetch_league_data_function.main import (
    fetch_league_data,
)


@pytest.fixture
def sample_league_data():
    return {
        "id": 1,
        "name": "Premier League",
        "code": "PL",
        "emblem": "logo_url",
        "currentSeason": {
            "startDate": "2023-08-01",
            "endDate": "2024-05-31",
            "currentMatchday": 25,
        },
        "teams": [
            {
                "id": 1,
                "name": "Test Team",
                "tla": "TTT",
                "crest": "team_logo_url",
                "venue": "Test Stadium",
                "address": "51.123,-0.456",
            }
        ],
    }


@pytest.mark.asyncio
async def test_fetch_league_data_success():
    with (
        patch(
            "cloud_functions.fetch_league_data_function.main.get_league_data"
        ) as mock_get_data,
        patch(
            "cloud_functions.fetch_league_data_function.main.load_data_into_bigquery"
        ) as mock_load_data,
    ):
        mock_get_data.return_value = sample_league_data()

        result = fetch_league_data(None, None)

        assert result == (
            "League data processed and pipeline triggered successfully.",
            200,
        )
        assert mock_get_data.call_count == 5
        mock_load_data.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_league_data_failure():
    with patch(
        "cloud_functions.fetch_league_data_function.main.get_league_data"
    ) as mock_get_data:
        mock_get_data.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            fetch_league_data(None, None)
