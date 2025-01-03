import os
import json
import logging
import asyncpraw
import asyncio
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import unicodedata
from datetime import timedelta
import time

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


class RateLimiter:
    def __init__(self, requests_per_minute=60):
        self.interval = 60 / requests_per_minute
        self.last_request = 0

    async def wait(self):
        now = time.time()
        elapsed = now - self.last_request
        if elapsed < self.interval:
            await asyncio.sleep(self.interval - elapsed)
        self.last_request = time.time()


class RedditCache:
    def __init__(self, bucket_name):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)
        self.cache = {}

    def get(self, match_id: str) -> Optional[Dict]:
        if match_id in self.cache:
            return self.cache[match_id]

        blob = self.bucket.blob(f"reddit_cache/{match_id}.json")
        if blob.exists():
            data = json.loads(blob.download_as_string())
            self.cache[match_id] = data
            return data
        return None


async def initialize_reddit():
    """Initialize Reddit API client with proper error handling."""
    try:
        if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
            raise ValueError(
                "Reddit API credentials are not set in environment variables."
            )

        reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent="soccer_match_analyzer v1.0",
        )
        # Test the connection
        await reddit.user.me()
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

    unprocessed_matches = []
    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)

    return unprocessed_matches


def clean_text(text: str) -> str:
    """Clean text by lowercasing and removing special characters."""
    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def generate_search_queries(match: Dict) -> List[str]:
    """Generate optimized search queries."""
    home_team = clean_text(match["home_team"])
    away_team = clean_text(match["away_team"])
    return [
        f'flair:"Match Thread" OR flair:"Post Match Thread" title:"{home_team}" AND title:"{away_team}"'
    ]


async def process_matches(matches, reddit):
    """Process matches in batches with rate limiting."""
    rate_limiter = RateLimiter(requests_per_minute=60)
    batch_size = 5

    for i in range(0, len(matches), batch_size):
        batch = matches[i : i + batch_size]
        tasks = [find_match_thread(reddit, match, rate_limiter) for match in batch]
        results = await asyncio.gather(*tasks)
        await asyncio.sleep(2)  # Pause between batches
        for result in results:
            yield result


async def find_match_thread(
    reddit, match: Dict, rate_limiter: RateLimiter
) -> Optional[Dict]:
    """Asynchronously find the Reddit thread corresponding to a football match."""
    logging.info(f"Searching Reddit threads for match ID {match['match_id']}")

    subreddit = await reddit.subreddit("soccer")
    match_date = match["utcDate"]
    time_filter_start = int((match_date - timedelta(days=1)).timestamp())
    time_filter_end = int((match_date + timedelta(days=2)).timestamp())

    search_queries = generate_search_queries(match)
    best_match = None
    highest_score: float = 0.0

    for query in search_queries:
        await rate_limiter.wait()
        try:
            async for submission in subreddit.search(query, sort="new", limit=50):
                if not (time_filter_start <= submission.created_utc <= time_filter_end):
                    continue

                title = clean_text(submission.title)
                home_team = clean_text(match["home_team"])
                away_team = clean_text(match["away_team"])

                score = max(
                    fuzz.token_set_ratio(title, f"{home_team} vs {away_team}"),
                    fuzz.token_set_ratio(title, f"{away_team} vs {home_team}"),
                )

                if score > highest_score:
                    highest_score = score
                    best_match = submission

        except Exception as e:
            logging.error(f"Error during Reddit search: {e}")
            continue

    if best_match and highest_score > 60:
        return await extract_thread_data(best_match, match["match_id"])
    return None


async def extract_thread_data(thread, match_id: int) -> Dict:
    """Asynchronously extract data from the Reddit thread."""
    logging.info(f"Extracting data from thread ID {thread.id}")
    try:
        await thread.comments.replace_more(limit=0)
        comments = []
        async for comment in thread.comments:
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
            "comments": comments[:10],  # Keep top 10 comments
        }
        return thread_data
    except Exception as e:
        logging.error(f"Error extracting thread data: {e}")
        return {}


async def save_to_gcs(data: Dict, match_id: int) -> None:
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
