import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.weather_data.convert_weather_data_function.main import (
    transform_to_parquet,
)


@pytest.fixture
def sample_event():
    message_data = {"action": "convert_weather"}
    encoded_data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    return {"data": encoded_data}


@pytest.fixture
def sample_context():
    return None


def test_transform_to_parquet_success(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {
                "BUCKET_NAME": "test-bucket",
                "GCP_PROJECT_ID": "test-project",
            },
        ),
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch("polars.DataFrame.write_parquet") as mock_write_parquet,
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.os.path.exists",
            return_value=True,
        ),
    ):
        mock_bucket = MagicMock()
        mock_blob_json = MagicMock()
        mock_blob_parquet = MagicMock()

        mock_blob_json.name = "weather_data/sample_weather.json"
        mock_blob_json.download_as_string.return_value = json.dumps(
            {
                "hourly": {
                    "temperature_2m": [15.0, 16.0, None],
                    "relativehumidity_2m": [80, None, 82],
                    "weathercode": [1, 2, 3],
                },
                "hourly_units": {
                    "temperature_2m": "°C",
                    "relativehumidity_2m": "%",
                    "weathercode": "wmo code",
                },
            }
        ).encode()

        mock_bucket.list_blobs.return_value = [mock_blob_json]
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        def blob_side_effect(path):
            if path == mock_blob_json.name:
                return mock_blob_json
            elif path == "weather_data_parquet/sample_weather.parquet":
                mock_blob_parquet.exists.return_value = False
                return mock_blob_parquet
            return MagicMock()

        mock_bucket.blob.side_effect = blob_side_effect

        mock_future = MagicMock()
        mock_future.result.return_value = "message-id"
        mock_publisher_instance = MagicMock()
        mock_publisher_instance.publish.return_value = mock_future
        mock_publisher.return_value = mock_publisher_instance

        result = transform_to_parquet(sample_event, sample_context)

        assert "Processed: 1, Skipped: 0, Errors: 0" in result
        mock_write_parquet.assert_called_once()
        assert mock_publisher_instance.publish.call_count == 2


def test_transform_to_parquet_no_json_files(sample_event, sample_context):
    with (
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = []
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance

        result = transform_to_parquet(sample_event, sample_context)

        assert "No JSON files found in weather_data folder" in result
        mock_publisher_instance.publish.assert_not_called()


def test_transform_to_parquet_invalid_message():
    event = {
        "data": base64.b64encode(
            json.dumps({"action": "invalid_action"}).encode("utf-8")
        )
    }
    context = None

    result = transform_to_parquet(event, context)

    assert "Invalid message format or incorrect action" in result


def test_transform_to_parquet_existing_parquet(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {
                "BUCKET_NAME": "test-bucket",
                "GCP_PROJECT_ID": "test-project",
            },
        ),
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch("polars.DataFrame.write_parquet") as mock_write_parquet,
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.os.path.exists",
            return_value=True,
        ),
    ):
        mock_bucket = MagicMock()
        mock_blob_json = MagicMock()
        mock_blob_parquet = MagicMock()

        mock_blob_json.name = "weather_data/sample_weather.json"
        mock_blob_json.download_as_string.return_value = json.dumps(
            {
                "hourly": {
                    "temperature_2m": [15.0, 16.0, None],
                    "relativehumidity_2m": [80, None, 82],
                    "weathercode": [1, 2, 3],
                },
                "hourly_units": {
                    "temperature_2m": "°C",
                    "relativehumidity_2m": "%",
                    "weathercode": "wmo code",
                },
            }
        ).encode()

        mock_bucket.list_blobs.return_value = [mock_blob_json]
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        def blob_side_effect(path):
            if path == mock_blob_json.name:
                return mock_blob_json
            elif path == "weather_data_parquet/sample_weather.parquet":
                mock_blob_parquet.exists.return_value = True
                return mock_blob_parquet
            return MagicMock()

        mock_bucket.blob.side_effect = blob_side_effect

        mock_future = MagicMock()
        mock_future.result.return_value = "message-id"
        mock_publisher_instance = MagicMock()
        mock_publisher_instance.publish.return_value = mock_future
        mock_publisher.return_value = mock_publisher_instance

        result = transform_to_parquet(sample_event, sample_context)

        assert "Processed: 0, Skipped: 1, Errors: 0" in result
        mock_write_parquet.assert_not_called()
        assert mock_publisher_instance.publish.call_count == 2


def test_transform_to_parquet_exception(sample_event, sample_context):
    with (
        patch(
            "cloud_functions.weather_data.convert_weather_data_function.main.storage.Client",
            side_effect=Exception("Storage Client Error"),
        ),
    ):
        result, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 500
        assert "Error in weather data conversion: Storage Client Error" in result
