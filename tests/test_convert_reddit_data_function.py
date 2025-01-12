import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.reddit_data.convert_reddit_data_function.main import (
    transform_to_parquet,
)


@pytest.fixture
def sample_event():
    message_data = {"action": "convert_reddit"}
    encoded_data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    return {"data": encoded_data}


@pytest.fixture
def sample_context():
    return None


def test_transform_to_parquet_success(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch("polars.DataFrame.write_parquet") as mock_write_parquet,
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.send_discord_notification"
        ),
    ):
        mock_bucket = MagicMock()
        mock_blob_json = MagicMock()
        mock_blob_parquet = MagicMock()

        mock_blob_json.name = "reddit_data/matches/sample_reddit_post.json"
        mock_blob_json.download_as_string.return_value = json.dumps(
            {
                "id": "abc123",
                "title": "Sample Reddit Post",
                "content": "This is a test post.",
            }
        ).encode()

        mock_bucket.list_blobs.return_value = [mock_blob_json]
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        def blob_side_effect(path):
            if path == mock_blob_json.name:
                return mock_blob_json
            elif path == "reddit_data_parquet/sample_reddit_post.parquet":
                mock_blob_parquet.exists.return_value = False
                return mock_blob_parquet
            return MagicMock()

        mock_bucket.blob.side_effect = blob_side_effect

        mock_future = MagicMock()
        mock_future.result.return_value = "message-id"
        mock_publisher_instance = MagicMock()
        mock_publisher_instance.publish.return_value = mock_future
        mock_publisher.return_value = mock_publisher_instance

        result_message, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 200
        assert "Processed 1 files, skipped 0 existing files" in result_message
        mock_write_parquet.assert_called_once()
        mock_publisher_instance.publish.assert_called()


def test_transform_to_parquet_no_json_files(sample_event, sample_context):
    with (
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch(
            "cloud_functions.reddit_data.convert_reddit_data_function.main.send_discord_notification"
        ),
    ):
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = []
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        mock_future = MagicMock()
        mock_future.result.return_value = "message-id"
        mock_publisher_instance = MagicMock()
        mock_publisher_instance.publish.return_value = mock_future
        mock_publisher.return_value = mock_publisher_instance

        result_message, status_code = transform_to_parquet(sample_event, sample_context)

        assert status_code == 200
        assert "No Reddit JSON files found to convert" in result_message
        mock_publisher_instance.publish.assert_called()


def test_transform_to_parquet_invalid_message():
    event = {
        "data": base64.b64encode(
            json.dumps({"action": "invalid_action"}).encode("utf-8")
        )
    }
    context = None

    with patch(
        "cloud_functions.reddit_data.convert_reddit_data_function.main.send_discord_notification"
    ):
        result_message, status_code = transform_to_parquet(event, context)

    assert status_code == 500
    assert "Invalid message format or missing action" in result_message
