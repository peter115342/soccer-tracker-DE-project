import os
import json
import logging
import praw
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import time
import unicodedata
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def get_competition_variations(competition: str) -> List[str]:
    """Generate variations of competition names"""
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
        "Bundesliga": [
            "German Bundesliga",
            "BL",
            "German League",
            "Germany",
            "German",
        ],
    }
    return variations.get(competition, [competition])


def clean_team_name(team_name: str) -> str:
    """Simplify team names for better matching while preserving key identifiers."""
    team_name = team_name.lower()
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-z\s]", "", team_name)

    remove_terms = [
        r"\b(fc|cf|sc|ac|club|cp|cd|ssd|aas|ssc|as|us|usl|sv|ss|kv|kvk|krc|afc|cfc)\b"
    ]

    for term in remove_terms:
        team_name = re.sub(term, "", team_name)

    team_name = re.sub(r"\s+", " ", team_name)
    return team_name.strip()


def initialize_reddit():
    """Initialize Reddit API client with enhanced validation and retry logic"""
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


def get_processed_matches() -> List[Dict]:
    """Fetch recent matches from BigQuery with enhanced filtering"""
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)

    logging.info("Fetching matches from BigQuery")

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
    logging.info(f"Found {len(matches)} total matches in BigQuery")

    unprocessed_matches = []
    for match in matches:
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)
            logging.info(
                f"Adding unprocessed match: {match['home_team']} vs {match['away_team']} "
                f"({match['home_score']}-{match['away_score']}) from {match['competition']}"
            )

    logging.info(f"Found {len(unprocessed_matches)} matches without Reddit data")
    return unprocessed_matches


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """Enhanced match thread search with improved matching logic."""
    logging.info(f"Searching for match: {match['home_team']} vs {match['away_team']}")

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"].date()

    home_team_full = match["home_team"].lower()
    away_team_full = match["away_team"].lower()
    home_team_clean = clean_team_name(match["home_team"])
    away_team_clean = clean_team_name(match["away_team"])
    next_days = match_date + timedelta(days=2)
    prev_days = match_date - timedelta(days=2)
    valid_dates = {prev_days, match_date, next_days}
    competition_variations = get_competition_variations(match["competition"])

    best_thread = None
    highest_score: float = 0

    search_queries = [
        f'flair:"Match Thread" {home_team_clean}',
        f'flair:"Match Thread" {away_team_clean}',
        f'flair:"Post Match Thread" {home_team_clean}',
        f'flair:"Post Match Thread" {away_team_clean}',
        f'flair:"Post-Match Thread" {home_team_clean}',
        f'flair:"Post-Match Thread" {away_team_clean}',
        'flair:"Match Thread"',
        'flair:"Post Match Thread"',
        'flair:"Post-Match Thread"',
        f"{home_team_clean} vs {away_team_clean}",
        f"{away_team_clean} vs {home_team_clean}",
        f'"{match["competition"].lower()}"',
        home_team_clean,
        away_team_clean,
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
        *[
            f'flair:"Post-Match Thread" {part}'
            for part in home_team_clean.split()
            if len(part) > 3
        ],
        *[
            f'flair:"Post-Match Thread" {part}'
            for part in away_team_clean.split()
            if len(part) > 3
        ],
    ]

    for search_query in search_queries:
        try:
            search_results = list(
                subreddit.search(
                    search_query,
                    sort="new",
                    time_filter="month",
                    syntax="lucene",
                    limit=200,
                )
            )

            for thread in search_results:
                thread_date = datetime.fromtimestamp(
                    thread.created_utc, tz=timezone.utc
                ).date()

                if thread_date not in valid_dates:
                    continue

                title_lower = thread.title.lower()

                competition_match = any(
                    comp.lower() in title_lower for comp in competition_variations
                )

                if not competition_match:
                    continue

                title_parts = re.split(r"vs\.?|v\.?|\||[-:]", title_lower)

                title_digits = re.findall(r"\b\d{1,2}\b", title_lower)

                date_match = (
                    str(match_date.day) in title_digits
                    or str(match_date.month) in title_digits
                )

                home_scores = [
                    fuzz.partial_ratio(home_team_full, part.strip())
                    for part in title_parts
                ] + [
                    fuzz.partial_ratio(home_team_clean, part.strip())
                    for part in title_parts
                ]

                away_scores = [
                    fuzz.partial_ratio(away_team_full, part.strip())
                    for part in title_parts
                ] + [
                    fuzz.partial_ratio(away_team_clean, part.strip())
                    for part in title_parts
                ]

                total_score = (max(home_scores) + max(away_scores)) / 2

                if date_match:
                    total_score += 10

                if total_score > highest_score and total_score > 25:
                    highest_score = total_score
                    best_thread = thread
                    logging.info(
                        f"New best match found: {thread.title} (Score: {total_score})"
                    )

        except Exception as e:
            logging.error(f"Error searching with query '{search_query}': {str(e)}")
            continue

    if best_thread:
        return extract_thread_data(best_thread)

    logging.info("No matching thread found")
    return None


def extract_thread_data(thread) -> Dict:
    """Extract relevant data from Reddit thread with enhanced comment handling"""
    logging.info(f"Extracting data from thread: {thread.title}")

    try:
        thread.comments.replace_more(limit=0)
        top_comments = []

        for comment in sorted(thread.comments, key=lambda x: x.score, reverse=True)[:5]:
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
            "thread_id": thread.id if thread.id is not None else "None",
            "title": thread.title if thread.title is not None else "None",
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

        logging.info(f"Extracted {len(top_comments)} top comments from thread")
        return thread_data

    except Exception as e:
        logging.error(f"Error extracting thread data: {str(e)}")
        return {
            "error": str(e),
            "thread_id": thread.id if thread.id is not None else "None",
        }


def save_to_gcs(thread_data: Dict, match_id: int) -> None:
    """Save Reddit thread data to GCS with validation"""
    if not thread_data:
        logging.error("No thread data to save")
        return

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
        except Exception as e:
            logging.error(f"Error saving to GCS: {str(e)}")
    else:
        logging.info(f"Reddit thread for match ID {match_id} already exists in GCS")
