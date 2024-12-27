import os
import json
import logging
import praw
from datetime import timedelta
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from fuzzywuzzy import fuzz
import time

logging.basicConfig(level=logging.INFO)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def initialize_reddit():
    """Initialize Reddit API client with validation"""
    required_vars = {
        "REDDIT_CLIENT_ID": REDDIT_CLIENT_ID,
        "REDDIT_CLIENT_SECRET": REDDIT_CLIENT_SECRET,
        "BUCKET_NAME": GCS_BUCKET_NAME,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent="soccer_match_analyzer",
    )


def get_processed_matches() -> List[Dict]:
    """Fetch matches from BigQuery matches_processed table and filter out ones that already have Reddit data in GCS"""
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    logging.info("Fetching matches from BigQuery matches_processed table")
    query = """
        SELECT 
            homeTeam.name as home_team,
            awayTeam.name as away_team,
            competition.name as competition,
            utcDate,
            id as match_id
        FROM sports_data_eu.matches_processed
        WHERE DATE(utcDate) = '2024-12-22'
        AND status = 'FINISHED'
        ORDER BY utcDate DESC
        LIMIT 5
    """

    matches = list(client.query(query).result())
    unprocessed_matches = []

    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)

    logging.info(f"Found {len(unprocessed_matches)} matches without Reddit data")
    return unprocessed_matches


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """Find matching Reddit thread for a specific match"""
    logging.info(
        f"Searching for Reddit thread for match: {match['home_team']} vs {match['away_team']}"
    )

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"]
    search_start = match_date - timedelta(hours=2)
    search_end = match_date + timedelta(hours=4)

    search_query = f'flair:"Match Thread" timestamp:{int(search_start.timestamp())}..{int(search_end.timestamp())}'
    logging.info(f"Using search query: {search_query}")

    try:
        threads = subreddit.search(search_query, sort="new", limit=50)
        time.sleep(1)
    except Exception as e:
        logging.error(f"Error during Reddit API call: {e}")
        return None

    for thread in threads:
        if is_matching_thread(thread.title, match):
            logging.info("Found matching thread:")
            logging.info(f" - Title: {thread.title}")
            logging.info(f" - URL: {thread.url}")
            logging.info(f" - Thread ID: {thread.id}")
            logging.info(f" - Created UTC: {thread.created_utc}")
            logging.info(f" - Author: {thread.author}")
            logging.info(f" - Score: {thread.score}")
            logging.info(f" - Number of Comments: {thread.num_comments}")
            return extract_thread_data(thread)

    logging.info(
        f"No matching thread found for {match['home_team']} vs {match['away_team']}"
    )
    return None


def is_matching_thread(title: str, match: Dict) -> bool:
    """Check if thread title matches match details using fuzzy matching"""
    title_lower = title.lower()
    home_team = match["home_team"].lower()
    away_team = match["away_team"].lower()
    competition = match["competition"].lower()

    home_words = [word for word in home_team.split() if len(word) > 3]
    away_words = [word for word in away_team.split() if len(word) > 3]

    home_match = any(fuzz.partial_ratio(word, title_lower) > 80 for word in home_words)
    away_match = any(fuzz.partial_ratio(word, title_lower) > 80 for word in away_words)
    comp_match = fuzz.partial_ratio(competition, title_lower) > 70

    return home_match and away_match and comp_match


def extract_thread_data(thread) -> Dict:
    """Extract relevant data from Reddit thread"""
    logging.info(f"Extracting data from thread: {thread.title}")
    thread.comments.replace_more(limit=0)
    top_comments = []

    for comment in sorted(thread.comments, key=lambda x: x.score, reverse=True)[:5]:
        top_comments.append(
            {
                "id": comment.id,
                "body": comment.body,
                "score": comment.score,
                "author": str(comment.author),
            }
        )

    logging.info(f"Extracted {len(top_comments)} top comments from thread")
    return {
        "thread_id": thread.id,
        "title": thread.title,
        "body": thread.selftext,
        "created_utc": thread.created_utc,
        "score": thread.score,
        "top_comments": top_comments,
    }


def save_to_gcs(thread_data: Dict, match_id: int) -> None:
    """Save Reddit thread data to GCS"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/{match_id}.json")

    if not blob.exists():
        blob.upload_from_string(
            data=json.dumps(thread_data), content_type="application/json"
        )
        logging.info(f"Saved Reddit thread for match ID {match_id} to GCS")
