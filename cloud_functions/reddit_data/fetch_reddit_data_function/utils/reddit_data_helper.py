import os
import json
import logging
import praw
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)

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
    """Fetch all Match Thread and Post Match Thread posts from r/soccer for a specific date"""
    subreddit = reddit.subreddit("soccer")

    start_datetime = datetime.strptime(date, "%Y-%m-%d")
    end_datetime = start_datetime + timedelta(days=1)
    start_timestamp = int(start_datetime.timestamp())
    end_timestamp = int(end_datetime.timestamp()) - 1

    seen_thread_ids = set()
    threads = []

    for flair in ["Match Thread", "Post Match Thread"]:
        query = f'flair:"{flair}" AND timestamp:{start_timestamp}..{end_timestamp}'

        for submission in subreddit.search(query, syntax="lucene", limit=100):
            if submission.id not in seen_thread_ids:
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

    return {"date": date, "threads": threads}


def save_to_gcs(data: dict, date: str) -> None:
    """Save the Reddit threads data to GCS"""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/raw/{date}.json")

    try:
        blob.upload_from_string(data=json.dumps(data), content_type="application/json")
        logging.info(f"Saved Reddit threads for date {date} to GCS")
    except Exception as e:
        logging.error(f"Error saving Reddit threads for date {date} to GCS: {e}")
        raise
