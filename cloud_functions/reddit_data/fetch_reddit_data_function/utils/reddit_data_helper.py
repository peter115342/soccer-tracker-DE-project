import os
import json
import logging
import praw
from google.cloud import storage
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = "r/soccer scraper by u/Immediate-Reward-287"


def initialize_reddit():
    """Initialize Reddit API client"""
    try:
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT,
        )
        return reddit
    except Exception as e:
        logging.error(f"Failed to initialize Reddit client: {e}")
        return None


def get_processed_matches():
    """Fetch processed matches from BigQuery"""
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = """
    SELECT
        id as match_id,
        utcDate,
        homeTeam.name as home_team,
        awayTeam.name as away_team,
        competition.name as competition
    FROM `matches_processed`
    WHERE status = 'FINISHED'
    """

    try:
        matches = []
        query_job = client.query(query)

        for row in query_job:
            match = {
                "match_id": row.match_id,
                "utcDate": row.utcDate,
                "home_team": row.home_team,
                "away_team": row.away_team,
                "competition": row.competition,
            }
            matches.append(match)

        return matches
    except Exception as e:
        logging.error(f"Error fetching matches from BigQuery: {e}")
        return []


def find_match_thread(reddit, match):
    """Find and extract data from match thread"""
    try:
        match_date = match["utcDate"].timestamp()
        subreddit = reddit.subreddit("soccer")
        search_query = f"title:\"Post Match Thread\" AND title:\"{match['home_team']}\" AND title:\"{match['away_team']}\""

        for submission in subreddit.search(
            search_query, sort="new", time_filter="week"
        ):
            post_date = submission.created_utc
            if abs(post_date - match_date) <= 86400:
                submission.comment_sort = "top"
                top_comments = []
                comment_count = 0

                submission.comments.replace_more(limit=0)
                for comment in submission.comments:
                    if comment_count >= 10:
                        break
                    top_comments.append(
                        {
                            "id": comment.id,
                            "body": comment.body,
                            "score": comment.score,
                            "author": str(comment.author),
                            "created_utc": int(comment.created_utc),
                        }
                    )
                    comment_count += 1

                thread_data = {
                    "match_id": match["match_id"],
                    "thread_id": submission.id,
                    "title": submission.title,
                    "body": submission.selftext,
                    "created_utc": int(submission.created_utc),
                    "score": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "num_comments": submission.num_comments,
                    "top_comments": top_comments,
                }
                return thread_data
        return None

    except Exception as e:
        logging.error(f"Error finding match thread: {e}")
        return None


def save_to_gcs(data: dict, match_id: int) -> None:
    """Save Reddit thread data to GCS"""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/{match_id}.json")

    if not blob.exists():
        try:
            blob.upload_from_string(
                data=json.dumps(data), content_type="application/json"
            )
            logging.info(f"Saved Reddit data for match ID {match_id} to GCS")
        except Exception as e:
            logging.error(
                f"Error saving Reddit data for match ID {match_id} to GCS: {e}"
            )
            raise
    else:
        logging.info(f"Reddit data for match ID {match_id} already exists in GCS")
