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


def fetch_reddit_threads(date: str) -> Dict[str, Any]:
    """
    Fetch Match Thread / Post Match Thread posts from r/soccer for a specific date.
    This example also catches posts whose title matches those phrases
    or if the author is MatchThreadder.
    """
    subreddit = reddit.subreddit("soccer")

    start_timestamp = int(datetime.strptime(date, "%Y-%m-%d").timestamp())
    end_timestamp = start_timestamp + 86400

    query = (
        "("
        '(flair:"Match Thread" OR flair:"match thread" OR '
        ' flair:"Post Match Thread" OR flair:"post match thread") '
        'OR (title:"Match Thread" OR title:"Post Match Thread") '
        'OR (author:"MatchThreadder")'
        ")"
    )

    VALID_FLAIRS = {
        "match thread",
        "post match thread",
        "Match Thread",
        "Post Match Thread",
    }
    KEY_TITLE_PHRASES = {"match thread", "post match thread"}
    SPECIAL_AUTHOR = "matchthreadder"

    threads = []
    try:
        for submission in subreddit.search(
            query=query,
            syntax="lucene",
            sort="new",
            time_filter="all",
            limit=None,
        ):
            if start_timestamp <= submission.created_utc <= end_timestamp:
                flair_text = (
                    submission.link_flair_text.lower().strip()
                    if submission.link_flair_text
                    else ""
                )
                title_text = submission.title.lower()
                author_name = (
                    str(submission.author).lower() if submission.author else ""
                )

                cond_flair = flair_text in VALID_FLAIRS
                cond_title = any(phrase in title_text for phrase in KEY_TITLE_PHRASES)
                cond_author = author_name == SPECIAL_AUTHOR

                if cond_flair or cond_title or cond_author:
                    thread_data = {
                        "thread_id": submission.id,
                        "title": submission.title,
                        "body": submission.selftext,
                        "created_utc": int(submission.created_utc),
                        "score": submission.score,
                        "upvote_ratio": submission.upvote_ratio,
                        "num_comments": submission.num_comments,
                        "flair": submission.link_flair_text,
                        "author": author_name,
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

    except Exception as e:
        logging.error(f"Error fetching threads for {date}: {str(e)}")

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
