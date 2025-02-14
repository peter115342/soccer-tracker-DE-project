import pytest
from unittest.mock import patch, MagicMock
from cloud_functions.serving_layer.sync_summaries_to_firestore_function.main import (
    sync_summaries_to_firestore,
)
from datetime import datetime


@pytest.fixture
def sample_storage_blob():
    mock_blob = MagicMock()
    mock_blob.name = "match_summaries/test_match.md"
    mock_blob.download_as_text.return_value = "Test match summary content"
    return [mock_blob]


def test_sync_summaries_to_firestore_success(sample_storage_blob):
    event = {
        "data": "eyJhY3Rpb24iOiAic3luY19zdW1tYXJpZXNfdG9fZmlyZXN0b3JlIn0="
    }  # base64 encoded message
    context = None

    with (
        patch("google.cloud.firestore.Client") as mock_firestore,
        patch("google.cloud.storage.Client") as mock_storage,
        patch.dict("os.environ", {"BUCKET_NAME": "test-bucket"}),
    ):
        # Setup Storage mock
        mock_storage_instance = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.return_value = sample_storage_blob
        mock_storage_instance.bucket.return_value = mock_bucket
        mock_storage.return_value = mock_storage_instance

        # Setup Firestore mock
        mock_firestore_instance = MagicMock()
        mock_collection = MagicMock()
        mock_doc = MagicMock()
        mock_doc_snapshot = MagicMock()
        mock_doc_snapshot.exists = False
        mock_doc.get.return_value = mock_doc_snapshot
        mock_firestore_instance.collection.return_value = mock_collection
        mock_collection.document.return_value = mock_doc
        mock_firestore.return_value = mock_firestore_instance

        result, status_code = sync_summaries_to_firestore(event, context)

        assert status_code == 200
        assert "Successfully synced" in result
        mock_bucket.list_blobs.assert_called_once()
        mock_collection.document.assert_called()
        mock_doc.set.assert_called()


def test_sync_summaries_to_firestore_invalid_message():
    event = {"data": "eyJhY3Rpb24iOiAiaW52YWxpZF9hY3Rpb24ifQ=="}
    context = None

    result, status_code = sync_summaries_to_firestore(event, context)

    assert status_code == 500
    assert "Invalid message format" in result


def test_sync_summaries_to_firestore_exception():
    event = {"data": "eyJhY3Rpb24iOiAic3luY19zdW1tYXJpZXNfdG9fZmlyZXN0b3JlIn0="}
    context = None

    with patch("google.cloud.storage.Client") as mock_storage:
        mock_storage.side_effect = Exception("Test exception")

        result, status_code = sync_summaries_to_firestore(event, context)

        assert status_code == 500
        assert "Error during Summaries Firestore sync" in result
