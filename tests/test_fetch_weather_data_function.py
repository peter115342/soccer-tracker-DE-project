import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import json
import base64

from cloud_functions.weather_data.fetch_weather_data_function.main import (
    fetch_weather_data,
    get_match_data,
)

GCP_PROJECT_ID = "your-gcp-project-id"
GCS_BUCKET_NAME = "your-bucket-name"


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", GCP_PROJECT_ID)
    monkeypatch.setenv("BUCKET_NAME", GCS_BUCKET_NAME)


@pytest.fixture
def sample_data():
    return {
        "data": base64.b64encode(
            json.dumps({"action": "fetch_weather"}).encode("utf-8")
        )
    }


def test_fetch_weather_data_no_action(mock_env_vars):
    data = {"data": base64.b64encode(json.dumps({}).encode("utf-8"))}
    result = fetch_weather_data(data, None)
    assert result == ("Invalid message format or incorrect action", 500)


@patch("cloud_functions.weather_data.fetch_weather_data_function.main.get_match_data")
@patch(
    "cloud_functions.weather_data.fetch_weather_data_function.main.pubsub_v1.PublisherClient"
)
@patch("cloud_functions.weather_data.fetch_weather_data_function.main.storage.Client")
def test_fetch_weather_data_no_matches(
    mock_storage_client,
    mock_publisher_client,
    mock_get_match_data,
    sample_data,
    mock_env_vars,
):
    mock_get_match_data.return_value = []
    mock_publisher_instance = mock_publisher_client.return_value
    mock_publisher_instance.topic_path.return_value = "projects/{}/topics/{}".format(
        GCP_PROJECT_ID, "convert_weather_to_parquet_topic"
    )

    result = fetch_weather_data(sample_data, None)
    assert result == ("No matches to process weather data for.", 200)
    mock_publisher_instance.publish.assert_called_once()


@patch(
    "cloud_functions.weather_data.fetch_weather_data_function.main.fetch_weather_by_coordinates"
)
@patch(
    "cloud_functions.weather_data.fetch_weather_data_function.main.save_weather_to_gcs"
)
@patch("cloud_functions.weather_data.fetch_weather_data_function.main.get_match_data")
@patch(
    "cloud_functions.weather_data.fetch_weather_data_function.main.pubsub_v1.PublisherClient"
)
@patch("cloud_functions.weather_data.fetch_weather_data_function.main.storage.Client")
def test_fetch_weather_data_with_matches(
    mock_storage_client,
    mock_publisher_client,
    mock_get_match_data,
    mock_save_weather_to_gcs,
    mock_fetch_weather,
    sample_data,
    mock_env_vars,
):
    mock_get_match_data.return_value = [
        {
            "id": 1,
            "utcDate": datetime.now().isoformat(),
            "homeTeam": {"address": "40.7128, -74.0060"},
        }
    ]

    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_blob.exists.return_value = False
    mock_bucket.blob.return_value = mock_blob
    mock_storage_client.return_value.bucket.return_value = mock_bucket

    mock_fetch_weather.return_value = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "hourly": {"time": ["2023-01-01T15:00"], "temperature_2m": [20.5]},
    }
    mock_save_weather_to_gcs.return_value = True

    mock_publisher_instance = mock_publisher_client.return_value
    mock_publisher_instance.topic_path.return_value = "projects/{}/topics/{}".format(
        GCP_PROJECT_ID, "convert_weather_to_parquet_topic"
    )
    mock_future = MagicMock()
    mock_future.result.return_value = "message-id"
    mock_publisher_instance.publish.return_value = mock_future

    result = fetch_weather_data(sample_data, None)
    assert result == ("Process completed.", 200)
    mock_fetch_weather.assert_called_once()
    mock_save_weather_to_gcs.assert_called_once()
    mock_publisher_instance.publish.assert_called_once()


@patch("cloud_functions.weather_data.fetch_weather_data_function.main.bigquery.Client")
def test_get_match_data(mock_bq_client, mock_env_vars):
    mock_query_job = MagicMock()
    mock_row = MagicMock()
    mock_row.match_id = 1
    mock_row.utcDate = datetime.now()
    mock_row.competition_code = "PL"
    mock_row.competition_name = "Premier League"
    mock_row.home_team_id = 100
    mock_row.home_team_name = "Home Team"
    mock_row.home_team_address = "Address"

    mock_query_job.result.return_value = [mock_row]
    mock_bq_client.return_value.query.return_value = mock_query_job

    matches = get_match_data()
    assert len(matches) == 1
    assert matches[0]["id"] == 1
    assert matches[0]["homeTeam"]["id"] == 100
    assert matches[0]["homeTeam"]["address"] == "Address"
