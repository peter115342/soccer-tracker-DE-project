import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.standings_data.fetch_standings_data_function.main import (
    fetch_standings_data,
)
import base64
import json


@pytest.fixture
def sample_event():
    message_data = {"action": "fetch_standings"}
    data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    event = {"data": data}
    context = None
    return event, context


def test_fetch_standings_data_success(sample_event):
    event, context = sample_event
    with (
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.get_unique_dates"
        ) as mock_get_unique_dates,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.get_processed_standings_dates"
        ) as mock_get_processed_dates,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.fetch_standings_for_date"
        ) as mock_fetch_standings_for_date,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.save_standings_to_gcs"
        ) as mock_save_standings_to_gcs,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_get_unique_dates.return_value = ["2023-01-01", "2023-01-02"]
        mock_get_processed_dates.return_value = ["2023-01-01"]
        mock_fetch_standings_for_date.return_value = [
            {
                "competitionId": 1,
                "competition": {"id": 1, "name": "Premier League", "code": "PL"},
                "season": {"id": 1, "startDate": "2023-08-01", "endDate": "2024-05-31"},
                "standings": [
                    {
                        "stage": "REGULAR_SEASON",
                        "type": "TOTAL",
                        "table": [
                            {
                                "position": 1,
                                "team": {"id": 1, "name": "Team A"},
                                "playedGames": 10,
                                "won": 8,
                                "draw": 1,
                                "lost": 1,
                                "points": 25,
                                "goalsFor": 20,
                                "goalsAgainst": 5,
                                "goalDifference": 15
                            }
                        ]
                    }
                ]
            }
        ]
        mock_save_standings_to_gcs.return_value = True

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_standings_data(event, context)

        assert status_code == 200
        assert result == "Process completed successfully."
        mock_get_unique_dates.assert_called_once()
        mock_get_processed_dates.assert_called_once()
        mock_fetch_standings_for_date.assert_called_once_with("2023-01-02")
        mock_save_standings_to_gcs.assert_called()
        mock_publisher_instance.publish.assert_called()


def test_fetch_standings_data_no_new_dates(sample_event):
    event, context = sample_event
    with (
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.get_unique_dates"
        ) as mock_get_unique_dates,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.get_processed_standings_dates"
        ) as mock_get_processed_dates,
        patch(
            "cloud_functions.standings_data.fetch_standings_data_function.main.pubsub_v1.PublisherClient"
        ) as mock_publisher,
    ):
        mock_get_unique_dates.return_value = ["2023-01-01"]
        mock_get_processed_dates.return_value = ["2023-01-01"]

        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = fetch_standings_data(event, context)

        assert status_code == 200
        assert result == "No new data to process"
        mock_get_unique_dates.assert_called_once()
        mock_get_processed_dates.assert_called_once()
        mock_publisher_instance.publish.assert_not_called()


def test_fetch_standings_data_invalid_action():
    message_data = {"action": "invalid_action"}
    data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    event = {"data": data}
    context = None

    result, status_code = fetch_standings_data(event, context)

    assert status_code == 500
    assert "Invalid message format or action" in result


def test_fetch_standings_data_exception(sample_event):
    event, context = sample_event
    with patch(
        "cloud_functions.standings_data.fetch_standings_data_function.main.get_unique_dates"
    ) as mock_get_unique_dates:
        mock_get_unique_dates.side_effect = Exception("Test exception")

        result, status_code = fetch_standings_data(event, context)

        assert status_code == 500
        assert "An error occurred" in result
        mock_get_unique_dates.assert_called_once()
