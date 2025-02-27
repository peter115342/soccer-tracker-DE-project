import pytest
import base64
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from cloud_functions.serving_layer.upcoming_matches_to_firestore_function.main import (
    sync_upcoming_matches_to_firestore,
)


@pytest.fixture
def sample_matches_response():
    return {
        "matches": [
            {
                "id": 12345,
                "utcDate": "2023-01-01T15:00:00Z",
                "status": "SCHEDULED",
                "homeTeam": {
                    "name": "Team A",
                    "crest": "https://example.com/teamA.png",
                },
                "awayTeam": {
                    "name": "Team B",
                    "crest": "https://example.com/teamB.png",
                },
                "competition": {
                    "id": 2021,
                    "name": "Premier League",
                    "emblem": "https://example.com/premier-league.png",
                },
                "area": {"name": "England", "flag": "https://example.com/england.svg"},
            }
        ]
    }


@pytest.fixture
def pubsub_event():
    message = '{"action": "sync_tomorrow_matches"}'
    return {"data": base64.b64encode(message.encode("utf-8"))}


def test_sync_upcoming_matches_success(sample_matches_response, pubsub_event):
    context = None
    tomorrow = (datetime.now() + timedelta(days=1)).date()

    with (
        patch("google.cloud.firestore.Client") as mock_firestore,
        patch("requests.get") as mock_requests,
    ):
        # Mock Firestore
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_doc = MagicMock()
        mock_firestore.return_value = mock_db
        mock_db.collection.return_value = mock_collection
        mock_collection.document.return_value = mock_doc

        # Mock API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_matches_response
        mock_requests.return_value = mock_response

        result, status_code = sync_upcoming_matches_to_firestore(pubsub_event, context)

        assert status_code == 200
        assert "Successfully synced" in result
        mock_db.collection.assert_called_once_with("upcoming_matches")
        mock_collection.document.assert_called_once_with(tomorrow.isoformat())
        mock_doc.set.assert_called_once()


def test_sync_upcoming_matches_invalid_message():
    context = None
    invalid_message = '{"action": "wrong_action"}'
    event = {"data": base64.b64encode(invalid_message.encode("utf-8"))}

    result, status_code = sync_upcoming_matches_to_firestore(event, context)

    assert status_code == 500
    assert "Invalid action specified" in result


def test_sync_upcoming_matches_firestore_error(pubsub_event):
    context = None

    with patch("google.cloud.firestore.Client") as mock_firestore:
        mock_firestore.side_effect = Exception("Firestore connection error")

        result, status_code = sync_upcoming_matches_to_firestore(pubsub_event, context)

        assert status_code == 500
        assert "Error during upcoming matches sync" in result

