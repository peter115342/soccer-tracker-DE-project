import os
import json
import logging
from datetime import datetime
import praw
from google.cloud import storage, bigquery
from typing import List, Dict, Any

logging.basicConfig(level=logging.WARNING)

reddit = praw.Reddit(
    client_id=os.environ.get("REDDIT_CLIENT_ID"),
    client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
    user_agent="Script to collect r/soccer threads",
)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def get_existing_dates_in_gcs() -> List[str]:
    """
    Get list of dates that already have JSON files in GCS.
    Returns dates extracted from filenames in reddit_data/raw/ directory.
    """
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    blobs = bucket.list_blobs(prefix="reddit_data/raw/")

    existing_dates = []
    for blob in blobs:
        filename = blob.name.split("/")[-1]
        if filename.endswith(".json"):
            date = filename[:-5]
            existing_dates.append(date)

    return existing_dates


def get_match_dates_from_bq() -> List[str]:
    """
    Fetch unique dates from matches_processed table in BigQuery.
    Excludes dates that already have JSON files in GCS.
    Returns dates in descending order to process newest matches first.
    """
    existing_dates = get_existing_dates_in_gcs()

    client = bigquery.Client()
    query = """
        SELECT DISTINCT DATE(utcDate) as match_date
        FROM `sports_data_eu.matches_processed`
        ORDER BY match_date ASC
    """
    query_job = client.query(query)

    all_dates = [row.match_date.strftime("%Y-%m-%d") for row in query_job]
    new_dates = [date for date in all_dates if date not in existing_dates]

    logging.warning(
        f"Found {len(new_dates)} new dates to process out of {len(all_dates)} total dates"
    )
    logging.warning(f"The dates are: {new_dates}")
    return new_dates


def fetch_reddit_threads(date: str) -> Dict[str, Any]:
    """
    Fetch Match Thread and Post Match Thread posts from r/soccer for a specific date.
    Filters posts based on their created_utc timestamp matching the given date.
    """
    subreddit = reddit.subreddit("soccer")

    start_timestamp = int(datetime.strptime(date, "%Y-%m-%d").timestamp())
    end_timestamp = start_timestamp + 86400

    threads = []
    for flair in ["match thread", "Post Match Thread"]:
        try:
            for submission in subreddit.search(
                query=f'flair:"{flair}"',
                syntax="lucene",
                sort="new",
                time_filter="all",
                limit=None,
            ):
                if start_timestamp <= submission.created_utc <= end_timestamp:
                    thread_data = {
                        "thread_id": submission.id,
                        "title": submission.title,
                        "body": submission.selftext,
                        "created_utc": int(submission.created_utc),
                        "score": submission.score,
                        "upvote_ratio": submission.upvote_ratio,
                        "num_comments": submission.num_comments,
                        "flair": flair,
                        "top_comments": [],
                    }

                    submission.comment_sort = "top"
                    submission.comments.replace_more(limit=0)
                    for comment in submission.comments[:10]:
                        thread_data["top_comments"].append(
                            {
                                "id": comment.id,
                                "body": comment.body,
                                "score": comment.score,
                                "author": str(comment.author),
                                "created_utc": int(comment.created_utc),
                            }
                        )

                    threads.append(thread_data)
                    logging.warning(f"Collected thread: {submission.title}")

        except Exception as e:
            logging.error(f"Error fetching {flair} threads for {date}: {str(e)}")
            continue

    result = {"date": date, "threads": threads, "thread_count": len(threads)}

    logging.warning(f"Collected {len(threads)} threads for date {date}")
    return result


def save_to_gcs(data: dict, date: str) -> None:
    """
    Save the Reddit threads data to GCS with error handling and logging.
    Creates a dated JSON file in the reddit_data/raw/ directory.
    """
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob_path = f"reddit_data/raw/{date}.json"
    blob = bucket.blob(blob_path)

    try:
        json_data = json.dumps(data, indent=2)
        blob.upload_from_string(data=json_data, content_type="application/json")
        logging.warning(
            f"Successfully saved {data['thread_count']} Reddit threads for {date} to GCS: {blob_path}"
        )
    except Exception as e:
        error_msg = f"Error saving Reddit threads for date {date} to GCS: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
