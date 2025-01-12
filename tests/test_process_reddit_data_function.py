import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
import json
from cloud_functions.reddit_data.process_reddit_data_function.main import (
    process_reddit_threads,
)


@pytest.fixture
def sample_raw_reddit_data():
    return {
        "threads": [
            {
                "thread_id": "abc123",
                "title": "Match Thread: Arsenal vs Chelsea",
                "body": "Premier League match discussion",
                "created_utc": int(datetime.now().timestamp()),
                "score": 100,
                "upvote_ratio": 0.95,
                "num_comments": 500,
                "flair": "Match Thread",
                "author": "user1",
                "url": "https://reddit.com/r/soccer/abc123",
                "top_comments": [
                    {
                        "id": "comment1",
                        "body": "Great match!",
                        "score": 50,
                        "author": "user2",
                        "created_utc": int(datetime.now().timestamp()),
                    }
                ],
            }
        ]
    }


@pytest.fixture
def sample_matches():
    return [
        {
            "id": "12345",
            "utcDate": "2024-01-01T20:00:00Z",
            "home_team": "Arsenal",
            "away_team": "Chelsea",
            "home_score": 2,
            "away_score": 1,
            "competition": "Premier League",
        }
    ]


def test_process_reddit_threads_success(sample_raw_reddit_data, sample_matches):
    event = {
        "data": "eyJhY3Rpb24iOiAicHJvY2Vzc19yZWRkaXQifQ=="  # base64 encoded {"action": "process_reddit"}
    }
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.process_reddit_data_function.main.storage.Client"
        ) as mock_storage,
        patch(
            "cloud_functions.reddit_data.process_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch(
            "cloud_functions.reddit_data.process_reddit_data_function.utils.reddit_processor.get_matches_for_date"
        ) as mock_get_matches,
        patch(
            "cloud_functions.reddit_data.process_reddit_data_function.utils.reddit_processor.validate_match_data"
        ) as mock_validate,
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
    ):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        mock_raw_blob = MagicMock()
        mock_raw_blob.name = "reddit_data/raw/2024-01-01.json"
        mock_raw_blob.exists.return_value = True
        mock_raw_blob.download_as_string.return_value = json.dumps(
            sample_raw_reddit_data
        ).encode("utf-8")

        mock_existing_blob = MagicMock()
        mock_existing_blob.name = "reddit_data/matches/12345.json"
        mock_existing_blob.download_as_string.return_value = json.dumps(
            {"match_id": "12345", "threads": []}
        ).encode("utf-8")

        def mock_list_blobs(prefix):
            if prefix.startswith("reddit_data/raw/"):
                return [mock_raw_blob]
            elif prefix.startswith("reddit_data/matches/"):
                return [mock_existing_blob]
            else:
                return []

        mock_bucket.list_blobs.side_effect = mock_list_blobs

        def mock_blob_getter(blob_name):
            if blob_name == "reddit_data/raw/2024-01-01.json":
                return mock_raw_blob
            elif blob_name == "reddit_data/matches/12345.json":
                return mock_existing_blob
            else:
                new_blob = MagicMock()
                new_blob.name = blob_name
                return new_blob

        mock_bucket.blob.side_effect = mock_blob_getter

        mock_get_matches.return_value = sample_matches
        mock_validate.return_value = {
            "valid": True,
            "passed": ["match_date", "home_team", "away_team"],
            "failed": [],
            "match_id": "12345",
        }

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = process_reddit_threads(event, context)

        assert status_code == 200
        assert "Processing completed successfully" in result


def test_process_reddit_threads_invalid_message():
    event = {
        "data": "eyJpbnZhbGlkIjogIm1lc3NhZ2UifQ=="  # base64 encoded {"invalid": "message"}
    }
    context = None

    with patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}):
        result, status_code = process_reddit_threads(event, context)

        assert status_code == 500
        assert "Invalid message format" in result


def test_process_reddit_threads_no_new_data(sample_raw_reddit_data, sample_matches):
    event = {"data": "eyJhY3Rpb24iOiAicHJvY2Vzc19yZWRkaXQifQ=="}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.process_reddit_data_function.main.storage.Client"
        ) as mock_storage,
        patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}),
    ):
        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket
        mock_bucket.list_blobs.return_value = []

        result, status_code = process_reddit_threads(event, context)

        assert status_code == 200
        assert "Processing completed successfully" in result
        mock_storage.assert_called_once()
        mock_bucket.list_blobs.assert_called_once()
