import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.fetch_match_data_function.utils.match_data_helper import (
    fetch_matches_for_competitions,
    save_to_gcs,
)


@pytest.fixture
def sample_api_response():
    return {
        "matches": [
            {
                "id": 1234,
                "competition": {"name": "Premier League"},
                "homeTeam": {"name": "Team A"},
                "awayTeam": {"name": "Team B"},
                "score": {"fullTime": {"home": 2, "away": 1}},
                "status": "FINISHED",
            }
        ]
    }


def test_fetch_matches_for_competitions():
    with (
        patch("requests.get") as mock_get,
        patch("google.cloud.storage.Client") as mock_storage,
    ):
        mock_get.return_value.json.return_value = sample_api_response()
        mock_get.return_value.status_code = 200

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        matches = fetch_matches_for_competitions("2023-01-01", "2023-01-02")

        assert len(matches) == 1
        assert matches[0]["id"] == 1234
        assert mock_get.call_count == len(["PL", "BL1", "PD", "SA", "FL1"])


def test_fetch_matches_rate_limit_handling():
    with (
        patch("requests.get") as mock_get,
        patch("google.cloud.storage.Client") as mock_storage,
        patch("time.sleep") as mock_sleep,
    ):
        mock_get.side_effect = [
            MagicMock(status_code=429),  # Rate limit response
            MagicMock(status_code=200, json=lambda: sample_api_response()),
        ]

        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        matches = fetch_matches_for_competitions("2023-01-01", "2023-01-02")

        mock_sleep.assert_called()
        assert len(matches) > 0


def test_save_to_gcs():
    with patch("google.cloud.storage.Client") as mock_storage:
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        test_data = {"id": 1234, "data": "test"}
        save_to_gcs(test_data, 1234)

        mock_blob.upload_from_string.assert_called_once()


def test_save_to_gcs_existing_match():
    with patch("google.cloud.storage.Client") as mock_storage:
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage.return_value.bucket.return_value = mock_bucket

        test_data = {"id": 1234, "data": "test"}
        save_to_gcs(test_data, 1234)

        mock_blob.upload_from_string.assert_not_called()
