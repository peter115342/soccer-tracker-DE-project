import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from cloud_functions.fetch_weather_data_function.utils.weather_data_helper import (
    fetch_weather_by_coordinates,
    save_weather_to_gcs,
)


@pytest.fixture
def sample_weather_response():
    return {
        "hourly": {
            "temperature_2m": [20.5],
            "relativehumidity_2m": [65],
            "precipitation": [0],
            "weathercode": [0],
            "pressure_msl": [1013],
            "cloudcover": [25],
            "windspeed_10m": [10],
            "winddirection_10m": [180],
        }
    }


def test_fetch_weather_by_coordinates_forecast():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = sample_weather_response()

        result = fetch_weather_by_coordinates(
            51.123, -0.456, datetime.now(timezone.utc)
        )

        assert "hourly" in result
        assert "temperature_2m" in result["hourly"]


def test_fetch_weather_by_coordinates_archive():
    with patch("requests.get") as mock_get:
        mock_get.side_effect = [
            MagicMock(status_code=200, json=lambda: sample_weather_response())
        ]

        past_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
        result = fetch_weather_by_coordinates(51.123, -0.456, past_date)

        assert "hourly" in result
        assert mock_get.call_count >= 1


def test_save_weather_to_gcs():
    with patch("google.cloud.storage.Client") as mock_storage:
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        test_data = {"hourly": {"temperature_2m": [20.5]}}
        result = save_weather_to_gcs(test_data, 1234)

        assert result is True
        mock_blob.upload_from_string.assert_called_once()


def test_save_weather_to_gcs_existing_data():
    with patch("google.cloud.storage.Client") as mock_storage:
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        test_data = {"hourly": {"temperature_2m": [20.5]}}
        result = save_weather_to_gcs(test_data, 1234)

        assert result is False
        mock_blob.upload_from_string.assert_not_called()
