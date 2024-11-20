import json
import logging
from typing import Any, Dict, List

import polars as pl
from google.cloud import bigquery, storage


def get_json_files_from_gcs(bucket_name: str, project_id: str) -> List[Dict[str, Any]]:
    """Fetches new JSON files from the GCS bucket and returns their contents as a list."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    bq_client = bigquery.Client(project=project_id)
    existing_matches_query = f"""
    SELECT DISTINCT id 
    FROM `{project_id}.sports_data.match_data`
    """
    existing_matches = set(
        row.id for row in bq_client.query(existing_matches_query).result()
    )
    logging.info(f"Found {len(existing_matches)} existing matches in match_data table")

    match_data = []
    blobs = bucket.list_blobs(prefix="match_data/")

    for blob in blobs:
        if blob.name.endswith(".json"):
            content = json.loads(blob.download_as_string())
            if content["id"] not in existing_matches:
                match_data.append(content)
                logging.info(f"Retrieved new match data for match {content['id']}")
            else:
                logging.info(f"Skipping already processed match {content['id']}")

    logging.info(f"Retrieved {len(match_data)} new matches for processing")
    return match_data


def process_match_data(match_data_list: List[Dict[str, Any]]) -> pl.DataFrame:
    """Transforms match data into a Polars DataFrame with the correct schema."""
    if not match_data_list:
        logging.info("No new matches to process")
        return pl.DataFrame()

    df = pl.DataFrame(match_data_list)

    df = df.with_columns(
        [
            pl.col("utcDate").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"),
            pl.col("lastUpdated").str.strptime(
                pl.Datetime, format="%Y-%m-%dT%H:%M:%SZ"
            ),
        ]
    )

    df = df.with_columns([pl.col("utcDate").dt.offset_by("1h").alias("cetDate")])

    df = df.with_columns(
        [
            pl.struct(["homeTeam"]).alias("homeTeam"),
            pl.struct(["awayTeam"]).alias("awayTeam"),
            pl.struct(["competition"]).alias("competition"),
            pl.struct(
                [
                    "score.winner",
                    "score.duration",
                    pl.struct(["score.fullTime.home", "score.fullTime.away"]).alias(
                        "fullTime"
                    ),
                    pl.struct(["score.halfTime.home", "score.halfTime.away"]).alias(
                        "halfTime"
                    ),
                ]
            ).alias("score"),
        ]
    )

    df = df.select(
        [
            "id",
            "utcDate",
            "cetDate",
            "status",
            "matchday",
            "stage",
            "lastUpdated",
            "homeTeam",
            "awayTeam",
            "competition",
            "score",
        ]
    )

    logging.info(f"Processed {len(df)} matches")
    return df


def transform_to_bigquery_rows(df: pl.DataFrame) -> List[Dict[str, Any]]:
    """Converts Polars DataFrame to BigQuery-compatible row format."""
    if df.is_empty():
        return []
    return df.to_dicts()
