import os
import json
import logging
import praw
import re
from datetime import timedelta
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from fuzzywuzzy import fuzz
import time

logging.basicConfig(level=logging.INFO)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")

# Team name variations mapping
TEAM_VARIATIONS = {
    "manchester united": ["man united", "man utd", "manchester utd", "mufc"],
    "manchester city": ["man city", "mcfc", "man. city"],
    "tottenham": ["tottenham hotspur", "spurs"],
    "wolverhampton": ["wolves", "wolverhampton wanderers"],
    "newcastle united": ["newcastle", "nufc"],
    "west ham": ["west ham united", "westham"],
    "leeds united": ["leeds", "lufc"],
    "nottingham forest": ["nottm forest", "forest", "nffc"],
    "paris saint-germain": ["psg", "paris sg", "paris saint germain"],
    "real madrid": ["madrid", "rmcf"],
    "barcelona": ["fc barcelona", "barca", "fcb"],
}


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
            utcDate,  # This was missing
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


def get_team_variations(team_name: str) -> List[str]:
    """Get all possible variations of a team name"""
    team_name = team_name.lower()
    for key, variations in TEAM_VARIATIONS.items():
        if team_name in [key] + variations:
            return [key] + variations
    return [team_name]


def find_match_thread(reddit, match: Dict) -> Optional[Dict]:
    """Find matching Reddit thread for a specific match with enhanced search"""
    logging.info(
        f"Searching for Reddit thread for match: {match['home_team']} vs {match['away_team']} ({match['home_score']}-{match['away_score']})"
    )

    subreddit = reddit.subreddit("soccer")
    match_date = match["utcDate"]
    search_start = match_date - timedelta(hours=6)
    search_end = match_date + timedelta(hours=24)

    max_retries = 3
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            search_query = f'title:"Match Thread" timestamp:{int(search_start.timestamp())}..{int(search_end.timestamp())}'
            logging.info(f"Using search query: {search_query}")

            threads = list(subreddit.search(search_query, sort="new", limit=100))
            logging.info(f"Found {len(threads)} potential threads")
            time.sleep(1)

            for thread in threads:
                if is_matching_thread(thread, match):
                    return extract_thread_data(thread)

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


def is_matching_thread(thread, match: Dict) -> bool:
    """Enhanced matching logic for Reddit match threads"""
    title_lower = thread.title.lower()
    body = thread.selftext.lower()
    home_team = match["home_team"].lower()
    away_team = match["away_team"].lower()
    competition = match["competition"].lower()

    title_patterns = [
        r"match thread:?\s*(.+?)\s*(?:vs\.?|v\.?|\-)\s*(.+?)(?:\s*\|\s*(.+))?$",
        r"match thread:?\s*(.+?)\s*(?:\d+\s*-\s*\d+)\s*(.+?)(?:\s*\|\s*(.+))?$",
    ]

    found_match = False
    for pattern in title_patterns:
        title_match = re.match(pattern, title_lower)
        if title_match:
            reddit_home_team = title_match.group(1).strip()
            reddit_away_team = title_match.group(2).strip()
            reddit_competition = (
                title_match.group(3).strip() if title_match.group(3) else ""
            )

            home_variations = get_team_variations(home_team)
            away_variations = get_team_variations(away_team)

            home_scores = [
                fuzz.token_set_ratio(var, reddit_home_team) for var in home_variations
            ]
            away_scores = [
                fuzz.token_set_ratio(var, reddit_away_team) for var in away_variations
            ]

            best_home_score = max(home_scores)
            best_away_score = max(away_scores)

            logging.debug(
                f"Best match scores - Home: {best_home_score}, Away: {best_away_score}"
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

            comp_match = True
            if reddit_competition:
                comp_match = fuzz.token_set_ratio(competition, reddit_competition) > 60
                logging.debug(
                    f"Competition match ratio: {fuzz.token_set_ratio(competition, reddit_competition)}"
                )

            if (
                best_home_score > 75
                and best_away_score > 75
                and comp_match
                and score_matches
            ):
                found_match = True
                logging.info(
                    f"Match found with confidence - Home: {best_home_score}%, Away: {best_away_score}%"
                )
                break

    return found_match


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
