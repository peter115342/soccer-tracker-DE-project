import pytest
from unittest.mock import patch
from cloud_functions.fetch_match_data_function.main import fetch_football_data


@pytest.fixture
def sample_matches():
    return [
        {
            "id": 1234,
            "competition": {"name": "Premier League"},
            "homeTeam": {"name": "Team A"},
            "awayTeam": {"name": "Team B"},
            "score": {"fullTime": {"home": 2, "away": 1}},
            "status": "FINISHED",
        }
    ]


@pytest.mark.asyncio
async def test_fetch_football_data_with_new_matches():
    with (
        patch(
            "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
        ) as mock_fetch,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_fetch.return_value = sample_matches()
        mock_publisher.return_value.topic_path.return_value = "test-topic"
        mock_publisher.return_value.publish.return_value.result.return_value = (
            "message-id"
        )

        result, status_code = fetch_football_data(None, None)

        assert status_code == 200
        assert result == "Process completed."


@pytest.mark.asyncio
async def test_fetch_football_data_no_matches():
    with (
        patch(
            "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
        ) as mock_fetch,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        mock_fetch.return_value = []
        mock_publisher.return_value.topic_path.return_value = "test-topic"

        result, status_code = fetch_football_data(None, None)

        assert status_code == 200
        assert "No new matches to process" in result


@pytest.mark.asyncio
async def test_fetch_football_data_error():
    with patch(
        "cloud_functions.fetch_match_data_function.main.fetch_matches_for_competitions"
    ) as mock_fetch:
        mock_fetch.side_effect = Exception("API Error")

        result, status_code = fetch_football_data(None, None)

        assert status_code == 500
        assert "An error occurred" in result
