import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.weather_data.weather_to_bigquery_function.main import (
    load_weather_to_bigquery,
)


@pytest.fixture
def sample_event():
    message_data = {"action": "load_weather_to_bigquery"}
    encoded_data = base64.b64encode(json.dumps(message_data).encode("utf-8"))
    return {"data": encoded_data}


@pytest.fixture
def sample_context():
    return None


def test_load_weather_to_bigquery_success(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.weather_data.weather_to_bigquery_function.main.bigquery.Client"
        ) as mock_bigquery_client,
    ):
        mock_client_instance = MagicMock()
        mock_dataset_ref = MagicMock()
        mock_query_job = MagicMock()

        mock_client_instance.dataset.return_value = mock_dataset_ref
        mock_client_instance.get_dataset.side_effect = Exception("Dataset not found")
        mock_client_instance.create_dataset.return_value = None
        mock_client_instance.get_table.side_effect = Exception("Table not found")
        mock_client_instance.create_table.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_query_job.result.return_value = iter([[1000]])

        mock_bigquery_client.return_value = mock_client_instance

        result_message, status_code = load_weather_to_bigquery(
            sample_event, sample_context
        )

        assert status_code == 200
        assert "External table 'weather_parquet' has been updated." in result_message
        assert "Total weather records available: 1000" in result_message

        mock_client_instance.create_dataset.assert_called_once()
        mock_client_instance.create_table.assert_called_once()
        mock_client_instance.update_table.assert_not_called()


def test_load_weather_to_bigquery_existing_table(sample_event, sample_context):
    with (
        patch.dict(
            "os.environ",
            {"BUCKET_NAME": "test-bucket", "GCP_PROJECT_ID": "test-project"},
        ),
        patch(
            "cloud_functions.weather_data.weather_to_bigquery_function.main.bigquery.Client"
        ) as mock_bigquery_client,
    ):
        mock_client_instance = MagicMock()
        mock_dataset_ref = MagicMock()
        mock_table = MagicMock()
        mock_query_job = MagicMock()

        mock_client_instance.dataset.return_value = mock_dataset_ref
        mock_client_instance.get_dataset.return_value = MagicMock()
        mock_client_instance.get_table.return_value = mock_table
        mock_client_instance.update_table.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_query_job.result.return_value = iter([[2000]])

        mock_bigquery_client.return_value = mock_client_instance

        result_message, status_code = load_weather_to_bigquery(
            sample_event, sample_context
        )

        assert status_code == 200
        assert "External table 'weather_parquet' has been updated." in result_message
        assert "Total weather records available: 2000" in result_message

        mock_client_instance.create_dataset.assert_not_called()
        mock_client_instance.create_table.assert_not_called()
        mock_client_instance.update_table.assert_called_once()


def test_load_weather_to_bigquery_invalid_message():
    event = {
        "data": base64.b64encode(
            json.dumps({"action": "invalid_action"}).encode("utf-8")
        )
    }
    context = None

    result_message, status_code = load_weather_to_bigquery(event, context)

    assert status_code == 500
    assert "Invalid message format" in result_message


def test_load_weather_to_bigquery_bigquery_exception(sample_event, sample_context):
    with (
        patch(
            "cloud_functions.weather_data.weather_to_bigquery_function.main.bigquery.Client"
        ) as mock_bigquery_client,
    ):
        mock_client_instance = MagicMock()
        mock_client_instance.dataset.side_effect = Exception("BigQuery error")
        mock_bigquery_client.return_value = mock_client_instance

        result_message, status_code = load_weather_to_bigquery(
            sample_event, sample_context
        )

        assert status_code == 500
        assert "Error during Weather BigQuery external table update" in result_message
