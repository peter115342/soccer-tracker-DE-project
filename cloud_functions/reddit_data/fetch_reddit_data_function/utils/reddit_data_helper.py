import os
import json
import logging
from datetime import datetime
from google.cloud import storage, bigquery
from typing import List, Dict, Any
import praw

logging.basicConfig(level=logging.INFO)

# Initialize Reddit API client
reddit = praw.Reddit(
    client_id=os.environ.get("REDDIT_CLIENT_ID"),
    client_secret=os.environ.get("REDDIT_CLIENT_SECRET"),
    user_agent="Script to collect r/soccer threads",
)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def get_match_dates_from_bq() -> List[str]:
    """Fetch unique dates from matches_processed table in BigQuery"""
    client = bigquery.Client()
    query = """
        SELECT DISTINCT DATE(utcDate) as match_date
        FROM `sports_data_eu.matches_processed`
        ORDER BY match_date
    """
    query_job = client.query(query)
    return [row.match_date.strftime("%Y-%m-%d") for row in query_job]


def fetch_reddit_threads(date: str) -> Dict[str, Any]:
    """Fetch ALL Match Thread and Post Match Thread posts from r/soccer for a specific date"""
    subreddit = reddit.subreddit("soccer")

    start_timestamp = int(datetime.strptime(date, "%Y-%m-%d").timestamp())
    end_timestamp = start_timestamp + 86400  # Add 24 hours in seconds

    seen_thread_ids = set()
    threads = []

    for flair in ["Match Thread", "Post Match Thread"]:
        search_query = f'flair:"{flair}"'

        for submission in subreddit.search(
            query=search_query,
            sort="new",
            time_filter="day",
            syntax="lucene",
            limit=None,
        ):
            if (
                start_timestamp <= submission.created_utc <= end_timestamp
                and submission.id not in seen_thread_ids
            ):
                seen_thread_ids.add(submission.id)
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

                seen_comment_ids = set()
                submission.comment_sort = "top"
                submission.comments.replace_more(limit=0)

                for comment in submission.comments[:10]:
                    if comment.id not in seen_comment_ids:
                        seen_comment_ids.add(comment.id)
                        comment_data = {
                            "id": comment.id,
                            "body": comment.body,
                            "score": comment.score,
                            "author": str(comment.author),
                            "created_utc": int(comment.created_utc),
                        }
                        thread_data["top_comments"].append(comment_data)

                threads.append(thread_data)
                logging.info(f"Processed thread {submission.id} for date {date}")

    result_data = {"date": date, "threads": threads}
    logging.info(f"Found {len(threads)} threads for date {date}")
    return result_data


def save_to_gcs(data: dict, date: str) -> None:
    """Save the Reddit threads data to GCS"""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/raw/{date}.json")

    try:
        blob.upload_from_string(data=json.dumps(data), content_type="application/json")
        logging.info(
            f"Saved {len(data['threads'])} Reddit threads for date {date} to GCS"
        )
    except Exception as e:
        logging.error(f"Error saving Reddit threads for date {date} to GCS: {e}")
        raise
