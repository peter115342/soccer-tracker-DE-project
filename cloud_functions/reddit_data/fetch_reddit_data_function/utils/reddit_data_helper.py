import os
import json
import logging
import asyncpraw
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import time
import asyncio
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


async def handle_ratelimit(reddit):
    limits = reddit.auth.limits
    remaining = limits.get("remaining", 0) if limits else 0
    reset_timestamp = limits.get("reset_timestamp") if limits else None

    if remaining is not None and remaining < 2 and reset_timestamp:
        sleep_time = max(reset_timestamp - time.time(), 0)
        if sleep_time > 0:
            logging.info(f"Rate limit reached. Waiting {sleep_time:.2f} seconds")
            await asyncio.sleep(sleep_time + 1)


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


async def initialize_reddit():
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

            reddit = asyncpraw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent="soccer_match_analyzer v1.0",
            )
            await reddit.user.me()
            logging.info("Successfully connected to Reddit API")
            return reddit

        except Exception as e:
            logging.error(
                f"Reddit initialization attempt {attempt + 1} failed: {str(e)}"
            )
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(5 * (attempt + 1))


def get_processed_matches() -> List[Dict]:
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    query = """
        SELECT
            homeTeam.name as home_team,
            awayTeam.name as away_team,
            competition.name as competition,
            utcDate,
            id as match_id,
            score.fullTime.homeTeam as home_score,
            score.fullTime.awayTeam as away_score
        FROM sports_data_eu.matches_processed
        WHERE score.fullTime.homeTeam IS NOT NULL
        AND score.fullTime.awayTeam IS NOT NULL
        ORDER BY utcDate DESC
    """

    matches = list(client.query(query).result())
    unprocessed_matches = []

    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)

    return unprocessed_matches


async def calculate_thread_match_score(thread, match: Dict, match_date) -> float:
    score: float = 0
    title_lower = thread.title.lower()

    match_datetime = match["utcDate"]
    thread_datetime = datetime.fromtimestamp(thread.created_utc, tz=timezone.utc)

    if thread_datetime.date() == match_datetime.date():
        score += 35
    else:
        return 0

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


async def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    logging.info(f"Searching for match: {match['home_team']} vs {match['away_team']}")

    subreddit = await reddit.subreddit("soccer")
    match_date = match["utcDate"].date()

    home_team_clean = clean_team_name(match["home_team"])
    away_team_clean = clean_team_name(match["away_team"])

    search_queries = [
        f'flair:"Match Thread" {home_team_clean}',
        f'flair:"Match Thread" {away_team_clean}',
        f'flair:"Post Match Thread" {home_team_clean}',
        f'flair:"Post Match Thread" {away_team_clean}',
        'flair:"Match Thread"',
        'flair:"Post Match Thread"',
        f'flair:"Match Thread" {home_team_clean} vs {away_team_clean}',
        f'flair:"Match Thread" {away_team_clean} vs {home_team_clean}',
        *[
            f'flair:"Match Thread" {part}'
            for part in home_team_clean.split()
            if len(part) > 3
        ],
        *[
            f'flair:"Match Thread" {part}'
            for part in away_team_clean.split()
            if len(part) > 3
        ],
        *[
            f'flair:"Post Match Thread" {part}'
            for part in home_team_clean.split()
            if len(part) > 3
        ],
        *[
            f'flair:"Post Match Thread" {part}'
            for part in away_team_clean.split()
            if len(part) > 3
        ],
    ]

    search_results = []
    for query in search_queries:
        await handle_ratelimit(reddit)
        try:
            async for submission in subreddit.search(
                query, sort="new", time_filter="week", syntax="lucene", limit=50
            ):
                search_results.append(submission)
        except Exception as e:
            logging.error(f"Error searching with query '{query}': {str(e)}")
            continue

    seen_ids = set()
    scored_threads = []

    for thread in search_results:
        if thread.id not in seen_ids:
            seen_ids.add(thread.id)
            score = await calculate_thread_match_score(thread, match, match_date)
            if score > 70:
                scored_threads.append((score, thread))

    if scored_threads:
        best_thread = max(scored_threads, key=lambda x: x[0])[1]
        return await extract_thread_data(best_thread, match["match_id"])

    return None


async def extract_thread_data(thread, match_id: int) -> Dict:
    logging.info(f"Extracting data from thread: {thread.title}")

    try:
        await thread.comments.replace_more(limit=0)
        top_comments = []

        comments = await thread.comments()
        sorted_comments = sorted(comments, key=lambda x: x.score, reverse=True)[:10]

        for comment in sorted_comments:
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
