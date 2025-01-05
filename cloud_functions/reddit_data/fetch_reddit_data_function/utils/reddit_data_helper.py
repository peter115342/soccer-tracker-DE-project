import os
import json
import logging
import praw
import re
from typing import List, Dict
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import time
from datetime import datetime, timezone
import unicodedata

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def get_competition_variations(competition: str) -> List[str]:
    variations = {
        "Primera Division": [
            "La Liga",
            "LALIGA",
            "Spanish Primera",
            "Primera División",
            "La Liga Santander",
            "Spanish La Liga",
            "Spanish League",
            "Spain",
        ],
        "Serie A": [
            "Italian Serie A",
            "Serie A TIM",
            "Calcio",
            "Italian League",
            "Italy",
            "Italian",
        ],
        "Ligue 1": [
            "French Ligue 1",
            "Ligue 1 Uber Eats",
            "French League",
            "France",
            "French",
        ],
        "Premier League": [
            "EPL",
            "English Premier League",
            "BPL",
            "PL",
            "English League",
            "England",
        ],
        "Bundesliga": ["German Bundesliga", "BL", "German League", "Germany", "German"],
    }
    return variations.get(competition, [competition])


def handle_ratelimit(reddit):
    limits = reddit.auth.limits or {}
    remaining = limits.get("remaining", 0)
    reset_timestamp = limits.get("reset_timestamp")

    if remaining is not None and remaining < 2 and reset_timestamp:
        sleep_time = max(reset_timestamp - time.time(), 0)
        if sleep_time > 0:
            logging.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds")
            time.sleep(sleep_time + 1)


def clean_team_name(team_name: str) -> str:
    team_name = team_name.lower()
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-z\s]", "", team_name)
    remove_terms = [
        r"\b(fc|cf|sc|ac|club|cp|cd|ssd|aas|ssc|as|us|usl|sv|ss|kv|kvk|krc|afc|cfc)\b"
    ]
    for term in remove_terms:
        team_name = re.sub(term, "", team_name)
    return re.sub(r"\s+", " ", team_name).strip()


def initialize_reddit():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            required_vars = {
                "REDDIT_CLIENT_ID": REDDIT_CLIENT_ID,
                "REDDIT_CLIENT_SECRET": REDDIT_CLIENT_SECRET,
                "BUCKET_NAME": GCS_BUCKET_NAME,
            }
            missing_vars = [var for var, value in required_vars.items() if not value]
            if missing_vars:
                raise ValueError(
                    f"Missing required environment variables: {', '.join(missing_vars)}"
                )

            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent="soccer_match_analyzer v1.0",
            )
            reddit.user.me()
            logging.info("Successfully connected to Reddit API")
            return reddit
        except Exception as e:
            logging.error(
                f"Reddit initialization attempt {attempt + 1} failed: {str(e)}"
            )
            if attempt == max_retries - 1:
                raise
            time.sleep(5 * (attempt + 1))


def get_processed_matches() -> Dict[datetime.date, List[Dict]]:
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    query = """
        SELECT DISTINCT
            DATE(utcDate) as match_date,
            ARRAY_AGG(STRUCT(
                homeTeam.name as home_team,
                awayTeam.name as away_team,
                competition.name as competition,
                utcDate,
                id as match_id,
                score.fullTime.homeTeam as home_score,
                score.fullTime.awayTeam as away_score
            )) as matches
        FROM sports_data_eu.matches_processed
        WHERE score.fullTime.homeTeam IS NOT NULL
        AND score.fullTime.awayTeam IS NOT NULL
        GROUP BY DATE(utcDate)
        ORDER BY match_date DESC
    """

    matches_by_date = {}
    query_results = client.query(query).result()

    blobs = list(bucket.list_blobs(prefix="reddit_data/"))
    existing_match_ids = {
        int(blob.name.split("/")[-1][:-5])
        for blob in blobs
        if blob.name.endswith(".json")
    }

    for row in query_results:
        matches = [
            match for match in row.matches if match.match_id not in existing_match_ids
        ]
        if matches:
            matches_by_date[row.match_date] = matches

    return matches_by_date


def fetch_threads_for_date(reddit, date: datetime.date) -> List[praw.models.Submission]:
    subreddit = reddit.subreddit("soccer")
    threads = []

    search_queries = ['flair:"Match Thread"', 'flair:"Post Match Thread"']

    for query in search_queries:
        handle_ratelimit(reddit)
        try:
            start_timestamp = int(
                datetime.combine(date, datetime.min.time()).timestamp()
            )
            end_timestamp = int(datetime.combine(date, datetime.max.time()).timestamp())

            date_query = f"{query} AND timestamp:{start_timestamp}..{end_timestamp}"
            results = subreddit.search(
                date_query, sort="new", syntax="lucene", limit=100
            )
            threads.extend(results)
        except Exception as e:
            logging.error(f"Error searching with query '{query}': {str(e)}")
            continue

    return list(set(threads))


def calculate_thread_match_score(thread, match: Dict, match_date) -> float:
    score = 0
    title_lower = thread.title.lower()

    thread_date = datetime.fromtimestamp(thread.created_utc, tz=timezone.utc).date()
    if thread_date == match_date:
        score += 30
    elif abs((thread_date - match_date).days) <= 1:
        score += 15

    home_score = fuzz.partial_ratio(match["home_team"].lower(), title_lower)
    away_score = fuzz.partial_ratio(match["away_team"].lower(), title_lower)
    score += (home_score + away_score) / 4

    if any(
        var.lower() in title_lower
        for var in get_competition_variations(match["competition"])
    ):
        score += 20

    if match.get("home_score") is not None and match.get("away_score") is not None:
        score_pattern = f"{match['home_score']}-{match['away_score']}"
        if score_pattern in title_lower:
            score += 15

    return score


def extract_thread_data(thread, match_id: int) -> Dict:
    logging.info(f"Extracting data from thread: {thread.title}")

    try:
        thread.comments.replace_more(limit=0)
        top_comments = []

        for comment in sorted(thread.comments, key=lambda x: x.score, reverse=True)[
            :10
        ]:
            if len(comment.body.strip()) > 10:
                top_comments.append(
                    {
                        "id": comment.id,
                        "body": comment.body if comment.body is not None else "None",
                        "score": comment.score,
                        "author": str(comment.author)
                        if comment.author is not None
                        else "None",
                        "created_utc": comment.created_utc
                        if comment.created_utc is not None
                        else 0,
                    }
                )

        thread_data = {
            "match_id": match_id,
            "thread_id": thread.id,
            "title": thread.title,
            "body": thread.selftext if thread.selftext is not None else "None",
            "created_utc": thread.created_utc,
            "score": thread.score,
            "upvote_ratio": thread.upvote_ratio
            if thread.upvote_ratio is not None
            else 0.0,
            "num_comments": thread.num_comments
            if thread.num_comments is not None
            else 0,
            "top_comments": top_comments,
        }

        return thread_data

    except Exception as e:
        logging.error(f"Error extracting thread data: {str(e)}")
        return {"error": str(e), "thread_id": thread.id, "match_id": match_id}


def save_to_gcs(thread_data: Dict, match_id: int) -> bool:
    if not thread_data:
        logging.error("No thread data to save")
        return False

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"reddit_data/{match_id}.json")

    if not blob.exists():
        try:
            blob.upload_from_string(
                data=json.dumps(thread_data, ensure_ascii=False),
                content_type="application/json",
            )
            logging.info(
                f"Successfully saved Reddit thread for match ID {match_id} to GCS"
            )
            return True
        except Exception as e:
            logging.error(f"Error saving to GCS: {str(e)}")
            return False
    else:
        logging.info(f"Reddit thread for match ID {match_id} already exists in GCS")
        return False
