import os
import json
import logging
import praw
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
from datetime import timedelta
import re
import unicodedata

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def initialize_reddit():
    """Initialize Reddit API client with proper error handling."""
    try:
        if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
            raise ValueError(
                "Reddit API credentials are not set in environment variables."
            )

        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent="soccer_match_analyzer v1.0",
        )
        reddit.user.me()  # Test connection
        logging.info("Successfully connected to Reddit API")
        return reddit
    except Exception as e:
        logging.error(f"Error initializing Reddit client: {e}")
        raise


def clean_text(text: str) -> str:
    """Clean and normalize text for comparison."""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def get_processed_matches() -> List[Dict]:
    """Fetch unprocessed matches from BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    query = """
        SELECT
            id AS match_id,
            homeTeam.name AS home_team,
            awayTeam.name AS away_team,
            competition.name AS competition,
            utcDate,
            score.fullTime.homeTeam AS home_score,
            score.fullTime.awayTeam AS away_score
        FROM `sports_data_eu.matches_processed`
        WHERE score.fullTime.homeTeam IS NOT NULL
        AND score.fullTime.awayTeam IS NOT NULL
    """

    matches = [dict(row.items()) for row in client.query(query)]
    logging.info(f"Found {len(matches)} total matches in BigQuery")

    unprocessed_matches = []
    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)
            logging.debug(f"Match ID {match['match_id']} is unprocessed")

    logging.info(f"Found {len(unprocessed_matches)} matches without Reddit data")
    return unprocessed_matches


def generate_search_queries(match: Dict) -> List[str]:
    """Generate optimized search queries for Reddit."""
    home = clean_text(match["home_team"])
    away = clean_text(match["away_team"])
    competition = clean_text(match["competition"])

    base_queries = [
        f"{home} vs {away}",
        f"{away} vs {home}",
        f"{home}-{away}",
        f"{away}-{home}",
    ]

    search_queries = []
    for flair in ["Match Thread", "Post Match Thread"]:
        for query in base_queries:
            search_queries.append(f'flair:"{flair}" {query}')
            if competition:
                search_queries.append(f'flair:"{flair}" {competition} {query}')

    return search_queries


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """Find the most relevant Reddit thread for a match."""
    logging.info(f"Searching Reddit threads for match ID {match['match_id']}")

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"]
    search_window = timedelta(hours=16)

    time_start = int((match_date - search_window).timestamp())
    time_end = int((match_date + search_window).timestamp())

    search_queries = generate_search_queries(match)
    best_thread = None
    highest_similarity = 0

    for query in search_queries:
        try:
            for submission in subreddit.search(query, sort="new", limit=10):
                if not (time_start <= submission.created_utc <= time_end):
                    continue

                title_similarity = fuzz.ratio(
                    clean_text(submission.title),
                    clean_text(f"{match['home_team']} vs {match['away_team']}"),
                )

                if title_similarity > highest_similarity:
                    highest_similarity = title_similarity
                    best_thread = submission

        except Exception as e:
            logging.error(f"Search error: {e}")
            continue

    if best_thread and highest_similarity > 75:
        return extract_thread_data(best_thread, match["match_id"])
    return None


def extract_thread_data(thread, match_id: int) -> Dict:
    """Extract and structure thread data."""
    logging.info(f"Extracting data from thread ID {thread.id}")
    try:
        thread.comments.replace_more(limit=0)

        top_comments = sorted(
            [comment for comment in thread.comments.list() if not comment.stickied],
            key=lambda x: x.score,
            reverse=True,
        )[:10]

        comments_data = [
            {
                "id": comment.id,
                "author": str(comment.author) if comment.author else "[deleted]",
                "body": comment.body,
                "score": comment.score,
                "created_utc": comment.created_utc,
            }
            for comment in top_comments
        ]

        thread_data = {
            "match_id": match_id,
            "thread_id": thread.id,
            "title": thread.title,
            "body": thread.selftext,
            "created_utc": thread.created_utc,
            "score": thread.score,
            "comment_count": thread.num_comments,
            "top_comments": comments_data,
        }
        return thread_data
    except Exception as e:
        logging.error(f"Error extracting thread data: {e}")
        return {}


def save_to_gcs(data: Dict, match_id: int) -> None:
    """Save thread data to GCS with duplicate handling."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/{match_id}.json")

    if blob.exists():
        logging.info(
            f"Data for match ID {match_id} already exists in GCS. Skipping save."
        )
        return

    try:
        blob.upload_from_string(data=json.dumps(data), content_type="application/json")
        logging.info(f"Data for match ID {match_id} saved to GCS.")
    except Exception as e:
        logging.error(f"Error saving data to GCS for match ID {match_id}: {e}")
        raise
