import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from cloud_functions.reddit_data.fetch_reddit_data_function.main import (
    fetch_reddit_data,
)


@pytest.fixture
def sample_dates():
    return ["2024-01-01", "2024-01-02"]


@pytest.fixture
def sample_reddit_data():
    return {
        "date": "2024-01-01",
        "threads": [
            {
                "thread_id": "abc123",
                "title": "Match Thread: Arsenal vs Chelsea",
                "body": "Match discussion",
                "created_utc": int(datetime.now(timezone.utc).timestamp()),
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
                        "author": "user1",
                        "created_utc": int(datetime.now(timezone.utc).timestamp()),
                    }
                ],
            }
        ],
        "thread_count": 1,
    }


def test_fetch_reddit_data_success(sample_dates, sample_reddit_data):
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.get_match_dates_from_bq"
        ) as mock_get_dates,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.fetch_reddit_threads"
        ) as mock_fetch_threads,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.save_to_gcs"
        ) as mock_save_to_gcs,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
    ):
        mock_get_dates.return_value = sample_dates
        mock_fetch_threads.return_value = sample_reddit_data
        mock_save_to_gcs.return_value = None

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 200
        assert "Process completed successfully" in result
        mock_get_dates.assert_called_once()
        assert mock_fetch_threads.call_count == len(sample_dates)
        assert mock_save_to_gcs.call_count == len(sample_dates)
        mock_publisher_instance.publish.assert_called_once()


def test_fetch_reddit_data_no_threads(sample_dates):
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.get_match_dates_from_bq"
        ) as mock_get_dates,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.fetch_reddit_threads"
        ) as mock_fetch_threads,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
    ):
        mock_get_dates.return_value = sample_dates
        mock_fetch_threads.return_value = {
            "date": "2024-01-01",
            "threads": [],
            "thread_count": 0,
        }

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 200
        assert "Process completed successfully" in result
        mock_get_dates.assert_called_once()
        assert mock_fetch_threads.call_count == len(sample_dates)
        mock_publisher_instance.publish.assert_called_once()


def test_fetch_reddit_data_exception():
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.get_match_dates_from_bq"
        ) as mock_get_dates,
    ):
        mock_get_dates.side_effect = Exception("Test exception")

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 500
        assert "An error occurred" in result
        mock_get_dates.assert_called_once()
