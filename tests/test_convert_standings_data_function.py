import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.standings_data.convert_standings_data_function.main import (
    transform_to_parquet,
)


@pytest.fixture
def sample_event():
    message_data = {"action": "convert_standings"}
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
            "cloud_functions.convert_standings_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.convert_standings_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch("polars.DataFrame.write_parquet") as mock_write_parquet,
    ):
        mock_bucket = MagicMock()
        mock_blob_json = MagicMock()
        mock_blob_parquet = MagicMock()

        mock_blob_json.name = "standings_data/sample_standings.json"
        mock_blob_json.download_as_string.return_value = json.dumps(
            {
                "fetchDate": "2023-09-01",
                "competitionId": "2000",
                "season": "2023",
                "standings": [
                    {
                        "type": "TOTAL",
                        "table": [
                            {
                                "position": 1,
                                "team": {"id": 1, "name": "Team A"},
                                "playedGames": 5,
                                "won": 4,
                                "draw": 1,
                                "lost": 0,
                                "points": 13,
                                "goalsFor": 10,
                                "goalsAgainst": 3,
                                "goalDifference": 7,
                            }
                        ],
                    }
                ],
            }
        ).encode()

        mock_bucket.list_blobs.return_value = [mock_blob_json]
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        def blob_side_effect(path):
            if path == mock_blob_json.name:
                return mock_blob_json
            elif path == "standings_data_parquet/sample_standings.parquet":
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
        assert "Processed 1 standings files, skipped 0 existing files" in result_message
        mock_write_parquet.assert_called_once()
        mock_publisher_instance.publish.assert_called()


def test_transform_to_parquet_no_json_files(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {
                "BUCKET_NAME": "test-bucket",
                "GCP_PROJECT_ID": "test-project",
            },
        ),
        patch(
            "cloud_functions.convert_standings_data_function.main.storage.Client"
        ) as mock_storage_client,
        patch(
            "cloud_functions.convert_standings_data_function.main.pubsub_v1.PublisherClient"
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
        assert "No standings JSON files found to convert" in result_message
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
