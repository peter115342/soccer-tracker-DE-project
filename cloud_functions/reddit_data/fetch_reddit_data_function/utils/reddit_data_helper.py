import os
import json
import logging
import praw
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import unicodedata
from datetime import timedelta

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
        # Test the connection
        reddit.user.me()
        logging.info("Successfully connected to Reddit API")
        return reddit
    except Exception as e:
        logging.error(f"Error initializing Reddit client: {e}")
        raise


def get_processed_matches() -> List[Dict]:
    """Fetch all matches from the 'matches_processed' table in BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    logging.info("Fetching all matches from BigQuery")

    query = """
        SELECT
            homeTeam.name AS home_team,
            awayTeam.name AS away_team,
            competition.name AS competition,
            utcDate,
            id AS match_id
        FROM
            `sports_data_eu.matches_processed`
        WHERE
            score.fullTime.homeTeam IS NOT NULL
            AND score.fullTime.awayTeam IS NOT NULL
    """

    query_job = client.query(query)
    matches = [dict(row.items()) for row in query_job]
    logging.info(f"Found {len(matches)} total matches in BigQuery")

    unprocessed_matches = []
    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)
            logging.debug(f"Match ID {match['match_id']} is unprocessed.")

    logging.info(f"Found {len(unprocessed_matches)} matches without Reddit data")
    return unprocessed_matches


def clean_text(text: str) -> str:
    """Clean text by lowercasing and removing special characters."""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def generate_search_queries(match: Dict) -> List[str]:
    """Generate a list of search queries for Reddit."""
    home_team = clean_text(match["home_team"])
    away_team = clean_text(match["away_team"])
    competition = clean_text(match["competition"])

    queries = [
        f'flair:"Match Thread" "{home_team}" "{away_team}"',
        f'flair:"Post Match Thread" "{home_team}" "{away_team}"',
        f'flair:"Match Thread" "{home_team}"',
        f'flair:"Match Thread" "{away_team}"',
        f'flair:"Match Thread" "{competition}" "{home_team}"',
        f'flair:"Match Thread" "{competition}" "{away_team}"',
        f'flair:"Post Match Thread" "{competition}" "{home_team}"',
        f'flair:"Post Match Thread" "{competition}" "{away_team}"',
    ]
    return queries


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """
    Find the Reddit thread corresponding to a football match.
    """
    logging.info(f"Searching Reddit threads for match ID {match['match_id']}")

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"]
    time_filter_start = int((match_date - timedelta(days=1)).timestamp())
    time_filter_end = int((match_date + timedelta(days=2)).timestamp())

    search_queries = generate_search_queries(match)
    best_match = None
    highest_score: float = 0.0

    for query in search_queries:
        logging.debug(f"Searching with query: {query}")
        try:
            results = subreddit.search(query, sort="new", limit=50)
            for thread in results:
                if not (time_filter_start <= thread.created_utc <= time_filter_end):
                    continue

                title = clean_text(thread.title)
                home_team = clean_text(match["home_team"])
                away_team = clean_text(match["away_team"])

                score = max(
                    fuzz.token_set_ratio(title, f"{home_team} vs {away_team}"),
                    fuzz.token_set_ratio(title, f"{away_team} vs {home_team}"),
                )

                if score > highest_score:
                    highest_score = score
                    best_match = thread

        except Exception as e:
            logging.error(f"Error during Reddit search: {e}")
            continue

    if best_match and highest_score > 60:
        logging.info(
            f"Best match thread found with score {highest_score}: {best_match.title}"
        )
        return extract_thread_data(best_match, match["match_id"])
    else:
        logging.info(
            f"No suitable Reddit thread found for match ID {match['match_id']}"
        )
        return None


def extract_thread_data(thread, match_id: int) -> Dict:
    """Extract data from the Reddit thread."""
    logging.info(f"Extracting data from thread ID {thread.id}")
    try:
        thread.comments.replace_more(limit=0)
        comments = []
        top_comments = sorted(
            thread.comments.list(), key=lambda x: x.score, reverse=True
        )[:10]

        for comment in top_comments:
            comments.append(
                {
                    "id": comment.id,
                    "author": str(comment.author) if comment.author else None,
                    "body": comment.body,
                    "score": comment.score,
                    "created_utc": comment.created_utc,
                }
            )

        thread_data = {
            "match_id": match_id,
            "thread_id": thread.id,
            "title": thread.title,
            "selftext": thread.selftext,
            "created_utc": thread.created_utc,
            "score": thread.score,
            "num_comments": thread.num_comments,
            "comments": comments,
        }
        return thread_data
    except Exception as e:
        logging.error(f"Error extracting thread data: {e}")
        return {}


def save_to_gcs(data: Dict, match_id: int) -> None:
    """Save the thread data to GCS if it doesn't already exist."""
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
