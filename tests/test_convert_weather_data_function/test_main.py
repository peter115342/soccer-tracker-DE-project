import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.convert_weather_data_function.main import transform_to_parquet


@pytest.fixture
def sample_weather_data():
    return {
        "hourly": {
            "temperature_2m": [20.5, 21.0],
            "relativehumidity_2m": [65, None],
            "weathercode": [1, None],
            "cloudcover": [80, None],
            "winddirection_10m": [180, None],
            "visibility": [10000, 9000],
        },
        "hourly_units": {"temperature_2m": "°C", "visibility": "m"},
    }


@pytest.mark.asyncio
async def test_transform_to_parquet_success():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "convert_weather"}).encode())
    }

    with (
        patch("google.cloud.storage.Client") as mock_storage,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [MagicMock(name="weather_data/test.json")]
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False
        mock_blob.download_as_string.return_value = json.dumps(
            sample_weather_data()
        ).encode()
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result = transform_to_parquet(input_data, None)

        assert "Processed" in result[0]
        assert "Skipped" in result[0]


@pytest.mark.asyncio
async def test_transform_to_parquet_no_files():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "convert_weather"}).encode())
    }

    with (
        patch("google.cloud.storage.Client") as mock_storage,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result = transform_to_parquet(input_data, None)

        assert "No JSON files found" in result[0]


@pytest.mark.asyncio
async def test_transform_to_parquet_invalid_message():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "invalid_action"}).encode())
    }

    result = transform_to_parquet(input_data, None)

    assert result[1] == 500
    assert "Invalid message format" in result[0]


@pytest.mark.asyncio
async def test_transform_to_parquet_data_cleaning():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "convert_weather"}).encode())
    }

    with (
        patch("google.cloud.storage.Client") as mock_storage,
    ):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [MagicMock(name="weather_data/test.json")]
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        test_data = sample_weather_data()
        mock_blob.download_as_string.return_value = json.dumps(test_data).encode()

        result = transform_to_parquet(input_data, None)

        assert result[1] != 500
