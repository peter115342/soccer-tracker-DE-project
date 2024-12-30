import os
import json
import logging
import praw
import re
from typing import List, Dict, Optional
from google.cloud import storage, bigquery
from rapidfuzz import fuzz, process
import time
import unicodedata
from datetime import datetime, timezone
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
            "Spanish League",
            "Spain",
        ],
        "Serie A": [
            "Italian Serie A",
            "Serie A TIM",
            "Calcio",
            "Italian League",
            "Italy",
        ],
        "Ligue 1": [
            "French Ligue 1",
            "Ligue 1 Uber Eats",
            "French League",
            "France",
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
        ],
    }
    return variations.get(competition, [competition])


def clean_team_name(team_name: str) -> str:
    """Simplify team names for better matching."""
    team_name = team_name.lower()
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-z\s]", "", team_name)
    team_name = re.sub(
        r"\b(fc|cf|sc|ac|united|city|club|cp|deportivo|real|cd|athletic|ssd|calcio|aas|ssc|as|udinese|torino|napoli|venezia|inter|internazionale|us|usl|sv|ss|kv|kvk|krc|afc|cfc|sporting|sport)\b",
        "",
        team_name,
    )
    team_name = re.sub(r"\s+", " ", team_name)
    team_name = team_name.strip()
    return team_name


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
    """Find the Reddit match thread for a given match."""
    logging.info(
        f"Searching for match: {match['home_team']} vs {match['away_team']} "
        f"({match['home_score']}-{match['away_score']})"
    )

    subreddit = reddit.subreddit("soccer")
    competition = match["competition"]
    competition_variations = get_competition_variations(competition)
    match_date = match["utcDate"].date()

    search_query = 'flair:"Match Thread"'

    max_retries = 3
    matching_threads = []

    for attempt in range(max_retries):
        try:
            logging.info(f"Searching subreddit with query: {search_query}")
            search_results = list(
                subreddit.search(
                    search_query,
                    sort="new",
                    time_filter="all",
                    syntax="lucene",
                    limit=1000,
                )
            )

            logging.info(f"Found {len(search_results)} results for query")

            for thread in search_results:
                thread_date = datetime.fromtimestamp(
                    thread.created_utc, tz=timezone.utc
                ).date()

                if thread_date != match_date:
                    continue

                match_score = is_matching_thread(thread, match, competition_variations)
                if match_score is not None:
                    matching_threads.append((match_score, thread))
                    logging.info(
                        f"Thread '{thread.title}' matched with score {match_score}"
                    )

            break

        except (RequestException, ResponseException) as e:
            if attempt == max_retries - 1:
                logging.error(f"Search failed for query '{search_query}': {str(e)}")
            time.sleep(2 * (attempt + 1))

    if matching_threads:
        best_match = max(matching_threads, key=lambda x: x[0])[1]
        logging.info(f"Best matching thread found: {best_match.title}")
        return extract_thread_data(best_match)

    logging.info("No matching thread found")
    return None


def is_matching_thread(
    thread, match: Dict, competition_variations: List[str]
) -> Optional[int]:
    """Check if a Reddit thread matches the given match."""
    title_lower = thread.title.lower()
    home_team = clean_team_name(match["home_team"])
    away_team = clean_team_name(match["away_team"])
    thread_competition = ""

    for comp_variation in competition_variations:
        if comp_variation.lower() in title_lower:
            thread_competition = comp_variation.lower()
            break

    if not thread_competition:
        return None

    title_team_names = re.split(r"vs\.?|v\.?|\||\-", title_lower)
    if len(title_team_names) < 2:
        return None

    title_teams = [clean_team_name(name.strip()) for name in title_team_names[:2]]

    team_scores = []
    for team_name in [home_team, away_team]:
        matches = process.extractOne(
            team_name,
            title_teams,
            scorer=fuzz.partial_ratio,
            score_cutoff=70,
        )
        if matches:
            team_scores.append(matches[1])
        else:
            return None

    total_score = sum(team_scores)
    return total_score


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
        return {
            "error": str(e),
            "thread_id": thread.id,
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
