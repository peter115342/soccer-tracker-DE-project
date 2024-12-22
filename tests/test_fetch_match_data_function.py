import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.fetch_match_data_function.main import fetch_football_data


@pytest.fixture
def sample_matches():
    return [
        {
            "id": 12345,
            "homeTeam": {"id": 1, "name": "Team A"},
            "awayTeam": {"id": 2, "name": "Team B"},
            "utcDate": "2023-01-01T15:00:00Z",
        }
    ]


def test_fetch_football_data_success(sample_matches):
    event = {}
    context = None
    with (
        patch(
            "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
        ) as mock_fetch_matches,
        patch(
            "cloud_functions.fetch_match_data_function.main.save_to_gcs"
        ) as mock_save_to_gcs,
        patch(
            "cloud_functions.fetch_match_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_fetch_matches.return_value = sample_matches
        mock_save_to_gcs.return_value = True
        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_football_data(event, context)

        assert status_code == 200
        assert result == "Process completed."
        mock_fetch_matches.assert_called_once()
        mock_save_to_gcs.assert_called()
        mock_publisher_instance.publish.assert_called()


def test_fetch_football_data_no_new_matches():
    event = {}
    context = None
    with (
        patch(
            "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
        ) as mock_fetch_matches,
        patch(
            "cloud_functions.fetch_match_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_fetch_matches.return_value = []
        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_football_data(event, context)

        assert status_code == 200
        assert result == "No new matches to process."
        mock_fetch_matches.assert_called_once()
        mock_publisher_instance.publish.assert_called()


def test_fetch_football_data_exception():
    event = {}
    context = None
    with (
        patch(
            "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
        ) as mock_fetch_matches,
    ):
        mock_fetch_matches.side_effect = Exception("Test exception")

        result, status_code = fetch_football_data(event, context)

        assert status_code == 500
        assert "An error occurred" in result
        mock_fetch_matches.assert_called_once()
