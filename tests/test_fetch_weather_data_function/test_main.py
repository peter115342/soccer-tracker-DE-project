import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from datetime import datetime
from cloud_functions.fetch_weather_data_function.main import (
    fetch_weather_data,
    get_match_data,
)


@pytest.fixture
def sample_matches():
    return [
        {
            "id": 1234,
            "utcDate": "2023-01-01T15:00:00Z",
            "homeTeam": {"id": 1, "name": "Test Team", "address": "51.123,-0.456"},
            "competition": {"code": "PL", "name": "Premier League"},
        }
    ]


@pytest.fixture
def sample_weather_data():
    return {
        "hourly": {
            "temperature_2m": [20.5],
            "relativehumidity_2m": [65],
            "precipitation": [0],
        }
    }


@pytest.mark.asyncio
async def test_fetch_weather_data_success():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "fetch_weather"}).encode())
    }

    with (
        patch(
            "cloud_functions.fetch_weather_data_function.main.get_match_data"
        ) as mock_get_matches,
        patch(
            "cloud_functions.fetch_weather_data_function.main.fetch_weather_by_coordinates"
        ) as mock_fetch_weather,
        patch(
            "cloud_functions.fetch_weather_data_function.main.save_weather_to_gcs"
        ) as mock_save,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_get_matches.return_value = sample_matches()
        mock_fetch_weather.return_value = sample_weather_data()
        mock_save.return_value = True
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result, status_code = fetch_weather_data(input_data, None)

        assert status_code == 200
        assert result == "Process completed."


@pytest.mark.asyncio
async def test_fetch_weather_data_no_matches():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "fetch_weather"}).encode())
    }

    with (
        patch(
            "cloud_functions.fetch_weather_data_function.main.get_match_data"
        ) as mock_get_matches,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_get_matches.return_value = []
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result, status_code = fetch_weather_data(input_data, None)

        assert status_code == 200
        assert "No matches to process" in result


def test_get_match_data():
    with patch("google.cloud.bigquery.Client") as mock_client:
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [
            MagicMock(
                match_id=1234,
                utcDate=datetime(2023, 1, 1, 15, 0),
                competition_code="PL",
                competition_name="Premier League",
                home_team_id=1,
                home_team_name="Test Team",
                home_team_address="51.123,-0.456",
            )
        ]
        mock_client.return_value.query.return_value = mock_query_job

        results = get_match_data()

        assert len(results) == 1
        assert results[0]["id"] == 1234
