import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.convert_match_data_function.main import transform_to_parquet


@pytest.fixture
def sample_event():
    message_data = {"action": "convert_matches"}
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
            "cloud_functions.convert_match_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.convert_match_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch("polars.DataFrame.write_parquet") as mock_write_parquet,
    ):
        mock_bucket = MagicMock()
        mock_blob_json = MagicMock()
        mock_blob_parquet = MagicMock()

        mock_blob_json.name = "match_data/sample_match.json"
        mock_blob_json.download_as_string.return_value = json.dumps(
            {
                "id": 1,
                "score": {"fullTime": {"homeTeam": 2, "awayTeam": 1}},
                "referees": [{"id": 100, "name": "Referee Name", "nationality": None}],
                "venue": None,
                "group": None,
                "season": {"winner": None},
            }
        ).encode()

        mock_bucket.list_blobs.return_value = [mock_blob_json]
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        def blob_side_effect(path):
            if path == mock_blob_json.name:
                return mock_blob_json
            elif path == "match_data_parquet/sample_match.parquet":
                mock_blob_parquet.exists.return_value = False
                return mock_blob_parquet
            return MagicMock()

        mock_bucket.blob = MagicMock(side_effect=blob_side_effect)

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
            "cloud_functions.convert_match_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.convert_match_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
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
        assert "No JSON files found to convert" in result_message
        mock_publisher_instance.publish.assert_called()


def test_transform_to_parquet_invalid_message():
    event = {
        "data": base64.b64encode(
            json.dumps({"action": "invalid_action"}).encode("utf-8")
        )
    }
    context = None

    result_message, status_code = transform_to_parquet(event, context)

    assert status_code == 500
    assert "Invalid message format" in result_message
