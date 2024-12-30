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
from datetime import datetime, timezone
from prawcore.exceptions import RequestException, ResponseException

logging.basicConfig(level=logging.INFO)

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
GCS_BUCKET_NAME = os.environ.get("BUCKET_NAME")


def get_competition_variations(competition: str) -> List[str]:
    """Generate variations of competition names"""
    variations = {
        "Primera Division": [
            "La Liga",
            "LaLiga",
            "Spanish Primera",
            "Primera División",
        ],
        "Serie A": ["Italian Serie A", "Serie A TIM", "Calcio"],
        "Ligue 1": ["French Ligue 1", "Ligue 1 Uber Eats"],
        "Premier League": ["EPL", "English Premier League", "BPL"],
        "Bundesliga": ["German Bundesliga"],
    }
    return variations.get(competition, [competition])


def clean_team_name(team_name: str) -> str:
    """Enhanced team name cleaning with more variations"""
    prefixes = {
        "fc",
        "cf",
        "fk",
        "ff",
        "sf",
        "fs",
        "fv",
        "vf",
        "ac",
        "ca",
        "aa",
        "af",
        "fa",
        "as",
        "sa",
        "ad",
        "da",
        "sc",
        "cs",
        "sv",
        "vs",
        "sp",
        "ps",
        "ss",
        "sk",
        "rc",
        "cr",
        "rcd",
        "rcf",
        "rca",
        "rsc",
        "rsb",
        "cd",
        "dc",
        "cp",
        "pc",
        "ce",
        "ec",
        "cv",
        "vc",
        "spvgg",
        "sgd",
        "sge",
        "sgf",
        "sgl",
        "bsc",
        "bsv",
        "bv",
        "vfb",
        "vfl",
        "vfr",
        "fsv",
        "tsv",
        "tsg",
        "psv",
        "cska",
        "zska",
        "csk",
        "fcs",
        "csm",
        "ogc",
        "ol",
        "psg",
        "rsca",
        "rkc",
        "aek",
        "paok",
        "pao",
        "asd",
        "usd",
        "asc",
        "usc",
        "acr",
        "acf",
        "acn",
        "pfc",
        "cfc",
        "spl",
        "spf",
        "scf",
        "ud",
        "us",
        "su",
        "un",
        "afc",
        "afa",
        "afb",
        "afs",
        "bk",
        "ks",
        "lks",
        "mks",
        "gks",
        "nk",
        "hk",
        "tk",
        "rb",
        "rk",
        "rv",
        "rw",
    }

    name_parts = team_name.lower().split()
    distinctive_parts = [part for part in name_parts if part not in prefixes]

    if distinctive_parts:
        if len(distinctive_parts) > 1 and len(distinctive_parts[-1]) > 3:
            team_name = " ".join(distinctive_parts[-2:])
        else:
            team_name = distinctive_parts[-1]

    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-zA-Z\s]", "", team_name)

    return team_name.strip()


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
    """Enhanced match thread finding with multiple search strategies"""
    logging.info(
        f"Searching for Reddit thread for match: {match['home_team']} vs {match['away_team']} ({match['home_score']}-{match['away_score']})"
    )

    subreddit = reddit.subreddit("soccer")
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])
    competition_variations = get_competition_variations(match["competition"])

    search_queries = [
        f'title:"{home_team}" AND title:"{away_team}"',
        f'title:"Match Thread" AND title:"{home_team}"',
        f'title:"Post Match Thread" AND title:"{home_team}"',
        f'title:"{match["home_score"]}-{match["away_score"]}" AND title:"{home_team}"',
        f'title:"{home_team} vs {away_team}"',
        f'title:"{home_team} v {away_team}"',
        f'title:"{home_team}-{away_team}"',
        f'title:"Match Thread: {home_team}"',
        f'selftext:"{home_team}" AND selftext:"{away_team}"',
    ]

    # Add competition-specific queries
    search_queries.extend(
        [
            f'title:"{home_team}" AND title:"{away_team}" AND title:"{comp}"'
            for comp in competition_variations
        ]
    )
    search_queries.extend(
        [
            f'title:"Match Thread" AND title:"{home_team}" AND title:"{comp}"'
            for comp in competition_variations
        ]
    )

    max_retries = 5
    retry_delay = 5
    matching_threads = []

    for attempt in range(max_retries):
        try:
            for query in search_queries:
                search_results = subreddit.search(
                    query, sort="new", time_filter="all", syntax="lucene", limit=100
                )

                for thread in search_results:
                    match_score = is_matching_thread(thread, match)
                    if match_score is not None:
                        matching_threads.append((match_score, thread))

            if matching_threads:
                best_match = max(matching_threads, key=lambda x: x[0])[1]
                return extract_thread_data(best_match)

            logging.info("No matching thread found")
            return None

        except (RequestException, ResponseException) as e:
            if attempt < max_retries - 1:
                sleep_time = retry_delay * (2**attempt)
                logging.warning(f"Rate limit hit, waiting {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logging.error(f"All attempts failed: {str(e)}")
                return None

    return None


def is_matching_thread(thread, match: Dict) -> Optional[int]:
    """Enhanced matching logic with more flexible scoring"""
    thread_date = datetime.fromtimestamp(thread.created_utc, tz=timezone.utc)
    match_date = match["utcDate"].replace(tzinfo=timezone.utc)

    if thread_date.date() != match_date.date():
        return None

    title_lower = thread.title.lower()
    body = thread.selftext.lower()
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])
    competition_variations = get_competition_variations(match["competition"])

    title_patterns = [
        r"(?:match|post match) thread:?\s*(.+?)\s*(?:vs\.?|v\.?|\-)\s*(.+?)(?:\s*\|\s*(.+))?$",
        r"(?:match|post match) thread:?\s*(.+?)\s*(?:\d+\s*[-–]\s*\d+)\s*(.+?)(?:\s*\|\s*(.+))?$",
        r"(.+?)\s*(?:vs\.?|v\.?|\-)\s*(.+?)(?:\s*\|\s*(.+?))?$",
        r"(.+?)\s+(?:\d+\s*[-–]\s*\d+)\s+(.+)$",
    ]

    score_patterns = [
        r"(?:ft|full.?time|ht|half.?time).*?(\d+)[-–](\d+)",
        r"(\d+)[-–](\d+)\s*(?:ft|full.?time)",
        r"score:?\s*(\d+)[-–](\d+)",
        r"^\s*(\d+)[-–](\d+)\s*$",
    ]

    for pattern in title_patterns:
        title_match = re.search(pattern, title_lower)
        if title_match:
            reddit_home_team = clean_team_name(title_match.group(1).strip())
            reddit_away_team = clean_team_name(title_match.group(2).strip())
            reddit_competition = (
                clean_team_name(title_match.group(3).strip())
                if len(title_match.groups()) >= 3 and title_match.group(3)
                else ""
            )

            home_score = fuzz.token_set_ratio(home_team, reddit_home_team)
            away_score = fuzz.token_set_ratio(away_team, reddit_away_team)

            competition_match = any(
                fuzz.token_set_ratio(clean_team_name(comp), reddit_competition) > 50
                for comp in competition_variations
            )

            score_matches = False
            for score_pattern in score_patterns:
                score_match = re.search(
                    score_pattern, body + title_lower, re.IGNORECASE
                )
                if score_match:
                    reddit_home_score = int(score_match.group(1))
                    reddit_away_score = int(score_match.group(2))
                    score_matches = (
                        reddit_home_score == match["home_score"]
                        and reddit_away_score == match["away_score"]
                    )
                    if score_matches:
                        break

            if (home_score > 30 and away_score > 30) or score_matches:
                score_bonus = 80 if score_matches else 0
                competition_bonus = 50 if competition_match else 0
                total_score = (
                    home_score
                    + away_score
                    + (150 if competition_match else 0)
                    + score_bonus
                    + competition_bonus
                )

                logging.info(f"Match found - Total score: {total_score}")
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
