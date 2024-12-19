import pytest
from unittest.mock import patch, MagicMock
import base64
import json
from cloud_functions.weather_to_bigquery_function.main import load_weather_to_bigquery


@pytest.mark.asyncio
async def test_load_weather_to_bigquery_success():
    input_data = {
        "data": base64.b64encode(
            json.dumps({"action": "load_weather_to_bigquery"}).encode()
        )
    }

    with (
        patch("google.cloud.bigquery.Client") as mock_bq,
    ):
        mock_bq.return_value.dataset.return_value = MagicMock()
        mock_bq.return_value.get_dataset.return_value = True
        mock_bq.return_value.get_table.return_value = MagicMock()

        # Mock query results for weather count
        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [(500,)]
        mock_bq.return_value.query.return_value = mock_query_job

        result, status_code = load_weather_to_bigquery(input_data, None)

        assert status_code == 200
        assert "External table 'weather_parquet'" in result
        assert "Total weather records available: 500" in result


@pytest.mark.asyncio
async def test_load_weather_to_bigquery_create_dataset():
    input_data = {
        "data": base64.b64encode(
            json.dumps({"action": "load_weather_to_bigquery"}).encode()
        )
    }

    with (
        patch("google.cloud.bigquery.Client") as mock_bq,
    ):
        mock_bq.return_value.dataset.return_value = MagicMock()
        mock_bq.return_value.get_dataset.side_effect = Exception("Dataset not found")
        mock_bq.return_value.get_table.return_value = MagicMock()

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [(0,)]
        mock_bq.return_value.query.return_value = mock_query_job

        result, status_code = load_weather_to_bigquery(input_data, None)

        assert status_code == 200
        assert mock_bq.return_value.create_dataset.called


@pytest.mark.asyncio
async def test_load_weather_to_bigquery_create_table():
    input_data = {
        "data": base64.b64encode(
            json.dumps({"action": "load_weather_to_bigquery"}).encode()
        )
    }

    with (
        patch("google.cloud.bigquery.Client") as mock_bq,
    ):
        mock_bq.return_value.dataset.return_value = MagicMock()
        mock_bq.return_value.get_dataset.return_value = True
        mock_bq.return_value.get_table.side_effect = Exception("Table not found")

        mock_query_job = MagicMock()
        mock_query_job.result.return_value = [(0,)]
        mock_bq.return_value.query.return_value = mock_query_job

        result, status_code = load_weather_to_bigquery(input_data, None)

        assert status_code == 200
        assert mock_bq.return_value.create_table.called


@pytest.mark.asyncio
async def test_load_weather_to_bigquery_invalid_message():
    input_data = {
        "data": base64.b64encode(json.dumps({"action": "invalid_action"}).encode())
    }

    result, status_code = load_weather_to_bigquery(input_data, None)

    assert status_code == 500
    assert "Invalid message format" in result
