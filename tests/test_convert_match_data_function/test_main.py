import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.convert_match_data_function.main import transform_to_parquet


@pytest.fixture
def sample_json_data():
    return [
        {
            "id": 1234,
            "score": {"fullTime": {"home": 2, "away": 1}},
            "referees": [{"id": 1, "nationality": None}],
            "venue": "Test Venue",
            "group": "Group A",
            "season": {"winner": "Team A"},
        }
    ]


@pytest.mark.asyncio
async def test_transform_to_parquet_success():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "convert_matches"}).encode())
    }

    with (
        patch("google.cloud.storage.Client") as mock_storage,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
        patch("builtins.open", create=True),
    ):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = [MagicMock(name="match_data/test.json")]
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False
        mock_blob.download_as_string.return_value = json.dumps(
            sample_json_data()
        ).encode()
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result, status_code = transform_to_parquet(input_data, None)

        assert status_code == 200
        assert "Processed" in result


@pytest.mark.asyncio
async def test_transform_to_parquet_no_files():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "convert_matches"}).encode())
    }

    with (
        patch("google.cloud.storage.Client") as mock_storage,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result, status_code = transform_to_parquet(input_data, None)

        assert status_code == 200
        assert "No JSON files found" in result


@pytest.mark.asyncio
async def test_transform_to_parquet_invalid_message():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "invalid_action"}).encode())
    }

    result, status_code = transform_to_parquet(input_data, None)

    assert status_code == 500
    assert "Invalid message format" in result
