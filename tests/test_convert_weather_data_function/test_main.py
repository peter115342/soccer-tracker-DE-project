import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.convert_weather_data_function.main import transform_to_parquet


@pytest.fixture
def sample_event():
    message_data = {"action": "convert_weather"}
    encoded_data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    return {"data": encoded_data}


@pytest.fixture
def mock_pubsub():
    with patch(
        "cloud_functions.convert_weather_data_function.main.pubsub_v1.PublisherClient"
    ) as mock:
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_instance.publish.return_value = mock_future
        mock_future.result.return_value = "message-id"
        mock.return_value = mock_instance
        yield mock


def test_transform_to_parquet_success(sample_event, sample_context, mock_pubsub):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.convert_weather_data_function.main.storage.Client"
        ) as mock_storage,
    ):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [MagicMock(name="weather_data/test.json")]
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False
        mock_blob.download_as_string.return_value = json.dumps(
            {"test": "data"}
        ).encode()

        result, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 200
        assert "Processed: 1, Skipped: 0" in result


def test_transform_to_parquet_no_json_files(sample_event, sample_context, mock_pubsub):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.convert_weather_data_function.main.storage.Client"
        ) as mock_storage,
    ):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        result, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 200
        assert "No weather JSON files found" in result


def test_transform_to_parquet_invalid_message():
    invalid_event = {
        "data": base64.b64encode(json.dumps({"action": "wrong_action"}).encode("utf-8"))
    }

    result, status_code = transform_to_parquet(invalid_event, None)

    assert status_code == 500
    assert "Invalid message format" in result


def test_transform_to_parquet_existing_parquet(
    sample_event, sample_context, mock_pubsub
):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.convert_weather_data_function.main.storage.Client"
        ) as mock_storage,
    ):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [MagicMock(name="weather_data/test.json")]
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        result, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 200
        assert "Processed: 0, Skipped: 1" in result


def test_transform_to_parquet_exception(sample_event, sample_context, mock_pubsub):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.convert_weather_data_function.main.storage.Client"
        ) as mock_storage,
    ):
        mock_storage.side_effect = Exception("Storage Client Error")

        result, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 500
        assert "Error during weather data conversion" in result
