import os
import json
import logging
import praw
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from fuzzywuzzy import fuzz
import time
import unicodedata

logging.basicConfig(level=logging.INFO)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def clean_team_name(team_name: str) -> str:
    """Normalize and clean team name"""
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-zA-Z\s]", "", team_name)
    team_name = team_name.lower()
    tokens = team_name.split()
    cleaned_tokens = [
        token
        for token in tokens
        if token not in ["fc", "cf", "sc", "afc", "cfc", "ac", "ss", "us"]
        and len(token) > 2
    ]
    return " ".join(cleaned_tokens)


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
    """Fetch matches from BigQuery matches_processed table with scores"""
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
            id as match_id,
            score.fullTime.homeTeam as home_score,
            score.fullTime.awayTeam as away_score
        FROM sports_data_eu.matches_processed
        WHERE status = 'FINISHED'
        ORDER BY utcDate DESC
    """

    matches = list(client.query(query).result())
    logging.info(f"Query returned {len(matches)} matches")

    unprocessed_matches = []
    for match in matches:
        logging.info(
            f"Match found: {match['home_team']} vs {match['away_team']} ({match['home_score']}-{match['away_score']})"
        )
        blob = bucket.blob(f"reddit_data/{match['match_id']}.json")
        if not blob.exists():
            unprocessed_matches.append(match)

    logging.info(f"Found {len(unprocessed_matches)} matches without Reddit data")
    return unprocessed_matches


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """Find matching Reddit thread for a specific match with enhanced search"""
    logging.info(
        f"Searching for Reddit thread for match: {match['home_team']} vs {match['away_team']} ({match['home_score']}-{match['away_score']})"
    )

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"]

    search_start = match_date.replace(hour=0, minute=0, second=0, microsecond=0)
    search_end = match_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            search_query = f'flair:"Match Thread" timestamp:{int(search_start.timestamp())}..{int(search_end.timestamp())}'
            logging.info(f"Using search query: {search_query}")

            threads = list(subreddit.search(search_query, sort="new", limit=300))
            logging.info(f"Found {len(threads)} Match Thread posts for the day")
            time.sleep(1)

            matching_threads = []
            for thread in threads:
                match_score = is_matching_thread(thread, match)
                if match_score is not None:
                    matching_threads.append((match_score, thread))

            if matching_threads:
                best_match = max(matching_threads, key=lambda x: x[0])[1]
                return extract_thread_data(best_match)

            return None

        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(
                    f"Attempt {attempt + 1} failed, retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logging.error(f"All attempts failed: {str(e)}")
                return None

    return None


def is_matching_thread(thread, match: Dict) -> Optional[int]:
    """Enhanced matching logic for Reddit match threads"""
    title_lower = thread.title.lower()
    body = thread.selftext.lower()
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])
    competition = clean_team_name(match["competition"])

    title_patterns = [
        r"match thread:?\s*(.+?)\s*(?:vs\.?|v\.?|\-)\s*(.+?)\s*(?:\|\s*(.+))?$",
        r"match thread:?\s*(.+?)\s*(?:\d+\s*[-–]\s*\d+)\s*(.+?)\s*(?:\|\s*(.+))?$",
    ]

    for pattern in title_patterns:
        title_match = re.match(pattern, title_lower)
        if title_match:
            reddit_home_team = clean_team_name(title_match.group(1).strip())
            reddit_away_team = clean_team_name(title_match.group(2).strip())
            reddit_competition = (
                clean_team_name(title_match.group(3).strip())
                if title_match.group(3)
                else ""
            )

            home_score = fuzz.token_set_ratio(home_team, reddit_home_team)
            away_score = fuzz.token_set_ratio(away_team, reddit_away_team)
            competition_score = (
                fuzz.token_set_ratio(competition, reddit_competition)
                if reddit_competition
                else 100
            )

            logging.debug(
                f"Match scores - Home: {home_score}, Away: {away_score}, Competition: {competition_score}"
            )

            score_pattern = r"(?:ft|full.?time|ht|half.?time).*?(\d+)[-–](\d+)"
            score_match = re.search(score_pattern, body, re.IGNORECASE)

            if score_match:
                reddit_home_score = int(score_match.group(1))
                reddit_away_score = int(score_match.group(2))
                score_matches = (
                    reddit_home_score == match["home_score"]
                    and reddit_away_score == match["away_score"]
                )
                logging.debug(
                    f"Score comparison: {reddit_home_score}-{reddit_away_score} vs {match['home_score']}-{match['away_score']}"
                )
            else:
                score_matches = False

            total_score = home_score + away_score + competition_score

            if (
                home_score > 60 and away_score > 60 and competition_score > 50
            ) or score_matches:
                logging.info(
                    f"Match found with confidence - Home: {home_score}%, Away: {away_score}%, Competition: {competition_score}%"
                )
                return total_score

    return None


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
    else:
        logging.info(f"Reddit thread for match ID {match_id} already exists in GCS")
