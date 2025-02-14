import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.serving_layer.sync_standings_to_firestore_function.main import (
    sync_standings_to_firestore,
)
from datetime import datetime


@pytest.fixture
def sample_bigquery_results():
    mock_row = MagicMock()
    mock_row.competitionId = 2021
    mock_row.fetchDate = datetime(2023, 1, 1)
    mock_row.seasonId = 734
    mock_row.currentMatchday = 38
    mock_row.standings = [
        {
            "position": 1,
            "teamId": 65,
            "teamName": "Manchester City FC",
            "teamShortName": "Man City",
            "teamTLA": "MCI",
            "teamCrest": "https://crests.football-data.org/65.png",
            "playedGames": 38,
            "points": 89,
            "won": 28,
            "draw": 5,
            "lost": 5,
            "goalsFor": 94,
            "goalsAgainst": 33,
            "goalDifference": 61,
        }
    ]
    return [mock_row]


def test_sync_standings_to_firestore_success(sample_bigquery_results):
    event = {
        "data": "eyJhY3Rpb24iOiAic3luY19zdGFuZGluZ3NfdG9fZmlyZXN0b3JlIn0="
    }  # base64 encoded message
    context = None

    with (
        patch("google.cloud.firestore.Client") as mock_firestore,
        patch("google.cloud.bigquery.Client") as mock_bigquery,
    ):
        # Setup BigQuery mock
        mock_bq_instance = MagicMock()
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = sample_bigquery_results
        mock_bq_instance.query.return_value = mock_query_job
        mock_bigquery.return_value = mock_bq_instance

        # Setup Firestore mock
        mock_firestore_instance = MagicMock()
        mock_collection = MagicMock()
        mock_doc = MagicMock()
        mock_firestore_instance.collection.return_value = mock_collection
        mock_collection.document.return_value = mock_doc
        mock_firestore.return_value = mock_firestore_instance

        result, status_code = sync_standings_to_firestore(event, context)

        assert status_code == 200
        assert "Successfully synced" in result
        mock_bq_instance.query.assert_called_once()
        mock_collection.document.assert_called()
        mock_doc.set.assert_called()


def test_sync_standings_to_firestore_invalid_message():
    event = {"data": "eyJhY3Rpb24iOiAiaW52YWxpZF9hY3Rpb24ifQ=="}
    context = None

    result, status_code = sync_standings_to_firestore(event, context)

    assert status_code == 500
    assert "Invalid message format" in result


def test_sync_standings_to_firestore_exception():
    event = {"data": "eyJhY3Rpb24iOiAic3luY19zdGFuZGluZ3NfdG9fZmlyZXN0b3JlIn0="}
    context = None

    with patch("google.cloud.bigquery.Client") as mock_bigquery:
        mock_bigquery.side_effect = Exception("Test exception")

        result, status_code = sync_standings_to_firestore(event, context)

        assert status_code == 500
        assert "Error during Standings Firestore sync" in result
