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
from prawcore.exceptions import RequestException, ResponseException

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
        ],
        "Serie A": ["Italian Serie A", "Serie A TIM", "Calcio", "Italian League"],
        "Ligue 1": ["French Ligue 1", "Ligue 1 Uber Eats", "French League"],
        "Premier League": ["EPL", "English Premier League", "BPL", "PL"],
        "Bundesliga": ["German Bundesliga", "BL", "German League"],
        "Champions League": ["UCL", "CL", "UEFA Champions League"],
        "Europa League": ["UEL", "EL", "UEFA Europa League"],
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

    team_name = team_name.lower()
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-z\s]", "", team_name)

    name_parts = team_name.split()
    distinctive_parts = [part for part in name_parts if part not in prefixes]

    if not distinctive_parts:
        return team_name

    if len(distinctive_parts) > 1 and len(distinctive_parts[-1]) > 3:
        team_name = " ".join(distinctive_parts[-2:])
    else:
        team_name = distinctive_parts[-1]

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
    """Enhanced match thread finding with improved search strategies"""
    logging.info(
        f"Searching for match: {match['home_team']} vs {match['away_team']} "
        f"({match['home_score']}-{match['away_score']})"
    )

    subreddit = reddit.subreddit("soccer")
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])

    search_queries = [
        f'title:"Match Thread" AND title:"{home_team}" AND title:"{away_team}"',
        f'title:"Post Match Thread" AND title:"{home_team}" AND title:"{away_team}"',
        f'title:"{match["home_score"]}-{match["away_score"]}" AND title:"{home_team}" AND title:"{away_team}"',
        f'title:"{home_team} vs {away_team}"',
        f'title:"{home_team} v {away_team}"',
    ]

    max_retries = 3
    matching_threads = []

    for query in search_queries:
        for attempt in range(max_retries):
            try:
                logging.info(f"Trying search query: {query}")
                search_results = list(
                    subreddit.search(
                        query, sort="new", time_filter="week", syntax="lucene", limit=10
                    )
                )

                logging.info(f"Found {len(search_results)} results for query")

                for thread in search_results:
                    match_score = is_matching_thread(thread, match)
                    if match_score is not None:
                        matching_threads.append((match_score, thread))

                break

            except (RequestException, ResponseException) as e:
                if attempt == max_retries - 1:
                    logging.error(f"Search failed for query '{query}': {str(e)}")
                time.sleep(2 * (attempt + 1))

    if matching_threads:
        best_match = max(matching_threads, key=lambda x: x[0])[1]
        logging.info(f"Best matching thread found: {best_match.title}")
        return extract_thread_data(best_match)

    logging.info("No matching thread found")
    return None


def is_matching_thread(thread, match: Dict) -> Optional[int]:
    """Enhanced matching logic with improved scoring system"""
    thread_date = datetime.fromtimestamp(thread.created_utc, tz=timezone.utc)
    match_date = match["utcDate"].replace(tzinfo=timezone.utc)

    if abs((thread_date - match_date).total_seconds()) > 86400:
        return None

    title_lower = thread.title.lower()
    body = thread.selftext.lower()
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])

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
            reddit_home = clean_team_name(title_match.group(1).strip())
            reddit_away = clean_team_name(title_match.group(2).strip())

            home_score = fuzz.token_set_ratio(home_team, reddit_home)
            away_score = fuzz.token_set_ratio(away_team, reddit_away)

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

            if (home_score > 80 and away_score > 80) or score_matches:
                total_score = home_score + away_score
                if score_matches:
                    total_score += 100
                if "match thread" in title_lower:
                    total_score += 50

                logging.info(f"Match found - Total score: {total_score}")
                return total_score

    return None


def extract_thread_data(thread) -> Dict:
    """Extract relevant data from Reddit thread with enhanced comment handling"""
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
                        "body": comment.body,
                        "score": comment.score,
                        "author": str(comment.author),
                        "created_utc": comment.created_utc,
                    }
                )

        thread_data = {
            "thread_id": thread.id,
            "title": thread.title,
            "body": thread.selftext,
            "created_utc": thread.created_utc,
            "score": thread.score,
            "upvote_ratio": thread.upvote_ratio,
            "num_comments": thread.num_comments,
            "top_comments": top_comments,
        }

        logging.info(f"Extracted {len(top_comments)} top comments from thread")
        return thread_data

    except Exception as e:
        logging.error(f"Error extracting thread data: {str(e)}")
        return None


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
