import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from cloud_functions.reddit_data.fetch_reddit_data_function.main import (
    fetch_reddit_data,
)


@pytest.fixture
def sample_matches():
    return [
        {
            "match_id": 12345,
            "home_team": "Arsenal",
            "away_team": "Chelsea",
            "competition": "Premier League",
            "utcDate": datetime.now(timezone.utc),
            "home_score": 2,
            "away_score": 1,
        }
    ]


@pytest.fixture
def sample_thread_data():
    return {
        "thread_id": "abc123",
        "title": "Match Thread: Arsenal vs Chelsea",
        "body": "Match discussion",
        "created_utc": datetime.now(timezone.utc).timestamp(),
        "score": 100,
        "upvote_ratio": 0.95,
        "num_comments": 500,
        "top_comments": [
            {
                "id": "comment1",
                "body": "Great match!",
                "score": 50,
                "author": "user1",
                "created_utc": datetime.now(timezone.utc).timestamp(),
            }
        ],
    }


def test_fetch_reddit_data_success(sample_matches, sample_thread_data):
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.initialize_reddit"
        ) as mock_init_reddit,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.get_processed_matches"
        ) as mock_get_matches,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.find_match_thread"
        ) as mock_find_thread,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.save_to_gcs"
        ) as mock_save_to_gcs,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_init_reddit.return_value = MagicMock()
        mock_get_matches.return_value = sample_matches
        mock_find_thread.return_value = sample_thread_data
        mock_save_to_gcs.return_value = None

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 200
        assert "Successfully processed 1 threads" in result
        assert "Success rate: 100.0%" in result
        mock_init_reddit.assert_called_once()
        mock_get_matches.assert_called_once()
        mock_find_thread.assert_called_once()
        mock_save_to_gcs.assert_called_once()
        mock_publisher_instance.publish.assert_called_once()


def test_fetch_reddit_data_no_matches():
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.initialize_reddit"
        ) as mock_init_reddit,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.get_processed_matches"
        ) as mock_get_matches,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.send_discord_notification"
        ) as mock_send_discord_notification,
        patch.dict("os.environ", {"GCP_PROJECT_ID": "test-project"}),
    ):
        mock_init_reddit.return_value = MagicMock()
        mock_get_matches.return_value = []

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 200
        assert "No matches found to process" in result
        mock_init_reddit.assert_called_once()
        mock_get_matches.assert_called_once()
        mock_publisher_instance.publish.assert_called_once()
        mock_send_discord_notification.assert_called_once()


def test_fetch_reddit_data_exception():
    event = {}
    context = None

    with (
        patch(
            "cloud_functions.reddit_data.fetch_reddit_data_function.main.initialize_reddit"
        ) as mock_init_reddit,
    ):
        mock_init_reddit.side_effect = Exception("Test exception")

        result, status_code = fetch_reddit_data(event, context)

        assert status_code == 500
        assert "Error fetching Reddit data" in result
        mock_init_reddit.assert_called_once()
