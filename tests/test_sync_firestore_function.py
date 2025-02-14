import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from cloud_functions.serving_layer.sync_to_firestore_function.main import (
    sync_matches_to_firestore,
)


@pytest.fixture
def sample_bigquery_row():
    class Row:
        def __init__(self):
            self.id = 12345
            self.utcDate = datetime(2023, 1, 1, 15, 0)
            self.status = "FINISHED"
            self.home_team = "Team A"
            self.away_team = "Team B"
            self.home_score = 2
            self.away_score = 1
            self.apparent_temperature = 20.5
            self.temperature_2m = 22.0
            self.precipitation = 0
            self.windspeed_10m = 10
            self.cloudcover = 50
            self.relativehumidity_2m = 65
            self.weathercode = 0
            self.venue = "Stadium"
            self.lat = 51.5074
            self.lon = -0.1278
            self.threads = None
            self.home_team_logo = "https://example.com/home_team_logo.png"
            self.away_team_logo = "https://example.com/away_team_logo.png"

    return [Row()]


def test_sync_matches_to_firestore_success(sample_bigquery_row):
    event = {
        "data": "eyJhY3Rpb24iOiAic3luY19tYXRjaGVzX3RvX2ZpcmVzdG9yZSJ9"  # base64 encoded
    }
    context = None

    with (
        patch("google.cloud.bigquery.Client") as mock_bq_client,
        patch("google.cloud.firestore.Client") as mock_firestore_client,
        patch("google.cloud.pubsub_v1.PublisherClient") as mock_publisher,
    ):
        # Setup BigQuery mock
        mock_query_job = MagicMock()
        mock_query_job.__iter__.return_value = sample_bigquery_row
        mock_bq_client.return_value.query.return_value = mock_query_job

        # Setup Firestore mock
        mock_collection = MagicMock()
        mock_doc = MagicMock()
        mock_firestore_client.return_value.collection.return_value = mock_collection
        mock_collection.document.return_value = mock_doc

        # Setup Pub/Sub mock
        mock_publisher_instance = MagicMock()
        mock_publisher.return_value = mock_publisher_instance
        mock_future = MagicMock()
        mock_future.result.return_value = "test-publish-id"
        mock_publisher_instance.publish.return_value = mock_future

        result, status_code = sync_matches_to_firestore(event, context)

        assert status_code == 200
        assert "Successfully synced" in result
        mock_bq_client.return_value.query.assert_called_once()
        mock_collection.document.assert_called_once_with("12345")
        mock_doc.set.assert_called_once()
        mock_publisher_instance.publish.assert_called_once()


def test_sync_matches_to_firestore_invalid_message():
    event = {
        "data": "eyJhY3Rpb24iOiAid3JvbmdfYWN0aW9uIn0="  # base64 encoded wrong action
    }
    context = None

    result, status_code = sync_matches_to_firestore(event, context)

    assert status_code == 500
    assert "Invalid message format" in result


def test_sync_matches_to_firestore_exception():
    event = {"data": "eyJhY3Rpb24iOiAic3luY19tYXRjaGVzX3RvX2ZpcmVzdG9yZSJ9"}
    context = None

    with patch("google.cloud.bigquery.Client") as mock_bq_client:
        mock_bq_client.side_effect = Exception("Test exception")

        result, status_code = sync_matches_to_firestore(event, context)

        assert status_code == 500
        assert "Error during Match Firestore sync" in result
