import os
import json
import logging
import asyncpraw
import re
from typing import List, Dict, Optional, Any
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import asyncio
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
    """
    Returns the list of known variations for a given competition name.
    These variations are used for fuzzy matching in thread titles.
    """
    variations_map = {
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
        "Bundesliga": [
            "German Bundesliga",
            "BL",
            "German League",
            "Germany",
            "German",
        ],
    }
    return variations_map.get(competition, [competition])


async def initialize_reddit() -> Optional[asyncpraw.Reddit]:
    """
    Initializes and returns an asyncpraw.Reddit client with a short retry mechanism.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET or not GCS_BUCKET_NAME:
                raise ValueError(
                    "Missing environment variables: REDDIT_CLIENT_ID, "
                    "REDDIT_CLIENT_SECRET, or BUCKET_NAME."
                )

            reddit = asyncpraw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent="reddit_match_thread_fetcher_v1",
            )
            _ = await reddit.user.me()
            logging.info("Successfully connected to Reddit API.")
            return reddit

        except Exception as ex:
            logging.error(f"Reddit initialization attempt {attempt + 1} failed: {ex}")
            if attempt == max_retries - 1:
                return None
            await asyncio.sleep(3 * (attempt + 1))
    return None


def get_processed_matches() -> List[Dict[str, Any]]:
    """
    Fetches all matches from 'sports_data_eu.matches_processed' in BigQuery
    that do not yet have a corresponding JSON file in GCS.

    Returns:
        A list of match records (dictionaries) that are unprocessed in GCS.
    """
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    query = """
        SELECT
            homeTeam.name AS home_team,
            awayTeam.name AS away_team,
            competition.name AS competition,
            utcDate,
            id AS match_id,
            score.fullTime.homeTeam AS home_score,
            score.fullTime.awayTeam AS away_score
        FROM `sports_data_eu.matches_processed`
        WHERE score.fullTime.homeTeam IS NOT NULL
          AND score.fullTime.awayTeam IS NOT NULL
        ORDER BY utcDate DESC
    """
    query_results = bigquery_client.query(query).result()

    unprocessed_matches = []
    for row in query_results:
        match_id = row["match_id"]
        blob_path = f"reddit_data/{match_id}.json"
        if not bucket.blob(blob_path).exists():
            unprocessed_matches.append(
                {
                    "match_id": match_id,
                    "home_team": row["home_team"],
                    "away_team": row["away_team"],
                    "competition": row["competition"],
                    "utcDate": row["utcDate"],
                    "home_score": row["home_score"],
                    "away_score": row["away_score"],
                }
            )

    logging.info(f"Found {len(unprocessed_matches)} unprocessed matches.")
    return unprocessed_matches


def clean_team_name(team_name: str) -> str:
    """
    Cleans the team name for improved fuzzy matching.
    Removes diacritics, punctuation, and common short terms (FC, AC, etc.).
    """
    team_name = (
        unicodedata.normalize("NFKD", team_name)
        .encode("ascii", "ignore")
        .decode("utf-8")
    )
    team_name = team_name.lower()

    team_name = re.sub(
        r"\b(fc|cf|sc|ac|club|cp|cd|ssd|aas|ssc|as|us|usl|sv|ss|kv|kvk|krc|afc|cfc)\b",
        "",
        team_name,
    )
    team_name = re.sub(r"[^a-z0-9\s-]", "", team_name)
    team_name = re.sub(r"\s+", " ", team_name).strip()
    return team_name


async def handle_ratelimit(reddit: asyncpraw.Reddit) -> None:
    """
    Checks the current rate-limiting information from Reddit and sleeps if needed.
    """
    if not reddit.auth.limits:
        return

    remaining = reddit.auth.limits.get("remaining", 0)
    reset_timestamp = reddit.auth.limits.get("reset_timestamp")

    if remaining <= 1 and reset_timestamp:
        wait_time = max(reset_timestamp - time.time(), 0)
        if wait_time > 0:
            logging.info(
                f"Rate limit reached. Sleeping for {wait_time + 1:.2f} seconds."
            )
            await asyncio.sleep(wait_time + 1)


async def find_match_thread(
    reddit: asyncpraw.Reddit, match: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Searches r/soccer for a match thread that best matches the home/away teams
    and competition from the given match dictionary.

    Returns:
        A dictionary with match thread data (including top comments)
        if a suitable thread is found, otherwise None.
    """
    if not reddit:
        return None

    home_team_clean = clean_team_name(match["home_team"])
    away_team_clean = clean_team_name(match["away_team"])
    competition_variations = get_competition_variations(match["competition"])
    match_date = match["utcDate"].date()

    # Exhaustive list of search queries
    search_queries = [
        f'flair:"Match Thread" {home_team_clean}',
        f'flair:"Match Thread" {away_team_clean}',
        f'flair:"Post Match Thread" {home_team_clean}',
        f'flair:"Post Match Thread" {away_team_clean}',
        'flair:"Match Thread"',
        'flair:"Post Match Thread"',
        f'flair:"Match Thread" {home_team_clean} vs {away_team_clean}',
        f'flair:"Match Thread" {away_team_clean} vs {home_team_clean}',
    ]
    for token in home_team_clean.split():
        if len(token) > 3:
            search_queries.append(f'flair:"Match Thread" {token}')
            search_queries.append(f'flair:"Post Match Thread" {token}')
    for token in away_team_clean.split():
        if len(token) > 3:
            search_queries.append(f'flair:"Match Thread" {token}')
            search_queries.append(f'flair:"Post Match Thread" {token}')

    subreddit = await reddit.subreddit("soccer")

    all_results = []
    seen_ids = set()
    for query in search_queries:
        await handle_ratelimit(reddit)
        try:
            # Limit to 'week' for safety, but you can adjust time_filter as needed
            async for submission in subreddit.search(
                query, sort="new", time_filter="week", limit=50, syntax="lucene"
            ):
                if submission.id not in seen_ids:
                    seen_ids.add(submission.id)
                    all_results.append(submission)
        except Exception as ex:
            logging.warning(f"Search failed for query '{query}': {ex}")
            continue

    if not all_results:
        logging.info(f"No potential threads found for match ID {match['match_id']}")
        return None

    # Score the potential threads
    scored_submissions = []
    for submission in all_results:
        score = await _score_match_thread(
            submission, match, competition_variations, match_date
        )
        if score > 0:
            scored_submissions.append((score, submission))

    if not scored_submissions:
        logging.info(
            f"No suitable match threads matched scoring for ID {match['match_id']}"
        )
        return None

    best_thread = max(scored_submissions, key=lambda x: x[0])[1]
    return await _extract_thread_data(best_thread, match["match_id"])


async def _score_match_thread(
    submission: asyncpraw.Reddit.submission,
    match: Dict[str, Any],
    competition_variations: List[str],
    match_date,
) -> float:
    """
    Assigns a fuzzy match score to a submission for the given match.
    Return 0 if the submission date is not the same match day.
    """
    score = 0.0
    sub_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)

    if sub_time.date() != match_date:
        return 0.0

    title_lower = submission.title.lower()

    home_ratio = fuzz.partial_ratio(match["home_team"].lower(), title_lower)
    away_ratio = fuzz.partial_ratio(match["away_team"].lower(), title_lower)
    score += (home_ratio + away_ratio) / 2  # Weighted higher

    if any(variation.lower() in title_lower for variation in competition_variations):
        score += 20

    home_score = match.get("home_score")
    away_score = match.get("away_score")
    if home_score is not None and away_score is not None:
        potential_score_str = f"{home_score}-{away_score}"
        if potential_score_str in title_lower:
            score += 15

    if "match thread" in title_lower:
        score += 5
    if "post match thread" in title_lower:
        score += 5

    return score


async def _extract_thread_data(
    submission: asyncpraw.Reddit.submission, match_id: int
) -> Dict[str, Any]:
    """
    Extract the top-level data (title, body, scores) plus top 10 comments
    from the submission.

    Returns:
        A dictionary with the thread data, including `match_id`.
    """
    try:
        await submission.comments.replace_more(limit=0)

        # Collect top 10 comments by score
        sorted_comments = sorted(
            submission.comments.list(),
            key=lambda c: c.score if c.score else 0,
            reverse=True,
        )[:10]

        top_comments = []
        for cmt in sorted_comments:
            body_text = cmt.body if hasattr(cmt, "body") else None
            if body_text and len(body_text.strip()) > 10:
                top_comments.append(
                    {
                        "id": cmt.id,
                        "body": body_text,
                        "score": cmt.score,
                        "author": str(cmt.author) if cmt.author else "None",
                        "created_utc": cmt.created_utc,
                    }
                )

        return {
            "match_id": match_id,
            "thread_id": submission.id,
            "title": submission.title,
            "body": submission.selftext if submission.selftext else "",
            "created_utc": submission.created_utc,
            "score": submission.score,
            "upvote_ratio": submission.upvote_ratio if submission.upvote_ratio else 0.0,
            "num_comments": submission.num_comments if submission.num_comments else 0,
            "top_comments": top_comments,
        }
    except Exception as ex:
        logging.error(
            f"Error extracting thread data for submission {submission.id}: {ex}"
        )
        return {
            "match_id": match_id,
            "thread_id": submission.id,
            "error": str(ex),
        }


def save_to_gcs(thread_data: Dict[str, Any], match_id: int) -> bool:
    """
    Saves the thread_data to GCS as a JSON file named `reddit_data/<match_id>.json`.
    Skips upload if the file already exists.

    Returns:
        True if file is newly created, False if it already existed or error occurred.
    """
    if not thread_data:
        logging.error("No thread data provided to save.")
        return False

    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob_name = f"reddit_data/{match_id}.json"
    blob = bucket.blob(blob_name)

    if blob.exists():
        logging.info(f"Reddit thread for match ID {match_id} already exists in GCS.")
        return False

    try:
        blob.upload_from_string(
            data=json.dumps(thread_data, ensure_ascii=False),
            content_type="application/json",
        )
        logging.info(
            f"Saved Reddit thread for match ID {match_id} to GCS at {blob_name}."
        )
        return True
    except Exception as e:
        logging.error(f"Failed to save Reddit thread for {match_id} to GCS: {str(e)}")
        return False
