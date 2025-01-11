from google.cloud import storage, bigquery
from rapidfuzz import fuzz
import json
import re
import unicodedata
from typing import Dict, List, Optional
import logging
from datetime import datetime


def get_competition_variations(competition: str) -> List[str]:
    """Generate variations of competition names"""
    variations = {
        "Primera Division": [
            "Primera Division",
            "La Liga",
            "LALIGA",
            "Spanish Primera",
            "Primera División",
            "La Liga Santander",
            "Spanish La Liga",
        ],
        "Serie A": [
            "Serie A",
            "Italian Serie A",
            "Serie A TIM",
            "Calcio",
            "Italian League",
        ],
        "Ligue 1": ["Ligue 1", "French Ligue 1", "Ligue 1 Uber Eats", "French League"],
        "Premier League": [
            "Premier League",
            "EPL",
            "English Premier League",
            "BPL",
            "PL",
        ],
        "Bundesliga": ["Bundesliga", "German Bundesliga", "BL", "German League"],
    }
    return variations.get(competition, [competition])


def clean_team_name(team_name: str) -> str:
    """Simplify team names for better matching"""
    team_name = team_name.lower()
    team_name = unicodedata.normalize("NFKD", team_name)
    team_name = re.sub(r"[^a-z\s]", "", team_name)
    team_name = re.sub(
        r"\b(fc|cf|sc|ac|united|city|club|cp|deportivo|real|cd|athletic|ssd|calcio|aas|ssc|as)\b",
        "",
        team_name,
    )
    team_name = re.sub(r"\s+", " ", team_name)
    return team_name.strip()


def get_existing_matches(bucket) -> Dict[str, Dict]:
    """Get all existing matches and their thread IDs"""
    existing_matches = {}
    for blob in bucket.list_blobs(prefix="reddit_data/matches/"):
        if blob.name.endswith(".json"):
            match_data = json.loads(blob.download_as_string())
            match_id = match_data["match_id"]
            existing_matches[match_id] = {
                "thread_ids": {t["thread_id"] for t in match_data["threads"]},
                "data": match_data,
            }
    return existing_matches


def get_matches_for_date(date: str) -> List[Dict]:
    """Fetch matches from BigQuery for a specific date"""
    client = bigquery.Client()
    query = """
        SELECT 
            id,
            utcDate,
            homeTeam.name as home_team,
            awayTeam.name as away_team,
            score.fullTime.homeTeam as home_score,
            score.fullTime.awayTeam as away_score,
            competition.name as competition
        FROM `sports_data_eu.matches_processed`
        WHERE DATE(utcDate) = DATE(@date)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("date", "STRING", date)]
    )

    return [dict(row) for row in client.query(query, job_config=job_config)]


def validate_match_data(match_data: Dict, bucket_name: str) -> Dict:
    """Validate processed match data against BigQuery records"""
    client = bigquery.Client()

    thread_date = datetime.fromtimestamp(
        match_data["threads"][0]["created_utc"]
    ).strftime("%Y-%m-%d")

    query = """
        SELECT 
            id,
            DATE(utcDate) as match_date,
            homeTeam.name as home_team,
            awayTeam.name as away_team
        FROM `sports_data_eu.matches_processed`
        WHERE id = @match_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("match_id", "STRING", match_data["match_id"])
        ]
    )

    results = list(client.query(query, job_config=job_config))
    if not results:
        return {
            "valid": False,
            "reason": "Match ID not found",
            "match_id": match_data["match_id"],
        }

    bq_match = dict(results[0])

    validations = {
        "match_date": str(bq_match["match_date"]) == thread_date,
        "home_team": clean_team_name(bq_match["home_team"])
        in clean_team_name(match_data["home_team"]),
        "away_team": clean_team_name(bq_match["away_team"])
        in clean_team_name(match_data["away_team"]),
    }

    passed_validations = [k for k, v in validations.items() if v]
    failed_validations = [k for k, v in validations.items() if not v]

    return {
        "valid": all(validations.values()),
        "passed": passed_validations,
        "failed": failed_validations,
        "match_id": match_data["match_id"],
    }


def match_thread_to_match(thread: Dict, matches: List[Dict]) -> Optional[Dict]:
    """Match a Reddit thread to a match using enhanced fuzzy matching"""
    title = thread["title"].lower()
    body = thread["body"].lower()

    best_match = None
    highest_score: float = 0

    for match in matches:
        home_team = clean_team_name(match["home_team"])
        away_team = clean_team_name(match["away_team"])
        competition = match["competition"].lower()

        home_score = fuzz.token_set_ratio(home_team, title)
        away_score = fuzz.token_set_ratio(away_team, title)

        comp_variations = get_competition_variations(competition)
        comp_scores = [
            fuzz.partial_ratio(var.lower(), title)
            + fuzz.partial_ratio(var.lower(), body)
            for var in comp_variations
        ]
        comp_score = max(comp_scores) if comp_scores else 0

        match_score = (home_score + away_score + comp_score / 2) / 2.5

        if thread["flair"] == "Post Match Thread" and match["home_score"] is not None:
            score_pattern = f"{match['home_score']}-{match['away_score']}"
            reverse_pattern = f"{match['away_score']}-{match['home_score']}"

            if (
                score_pattern in title
                or score_pattern in body
                or reverse_pattern in title
                or reverse_pattern in body
            ):
                match_score += 30

        title_patterns = [
            f"{home_team} vs {away_team}",
            f"{home_team} v {away_team}",
            f"{away_team} vs {home_team}",
            f"{away_team} v {home_team}",
        ]
        if any(pattern in clean_team_name(title) for pattern in title_patterns):
            match_score += 15

        if match_score > highest_score and match_score > 75:
            highest_score = match_score
            best_match = match
            logging.info(
                f"Found match with score {match_score}: "
                f"{match['home_team']} vs {match['away_team']}"
            )

    return best_match


def process_reddit_data(date: str, bucket_name: str) -> Dict:
    """Process Reddit data and organize by match ID"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    raw_blob = bucket.blob(f"reddit_data/raw/{date}.json")

    if not raw_blob.exists():
        logging.info(f"No Reddit data found for date {date}")
        return {
            "date": date,
            "processed_threads": 0,
            "skipped_threads": 0,
            "validations": [],
        }

    existing_matches = get_existing_matches(bucket)
    reddit_data = json.loads(raw_blob.download_as_string())
    matches = get_matches_for_date(date)

    match_threads = {}
    skipped_threads = 0
    new_threads_processed = 0
    validation_results = []

    for thread in reddit_data["threads"]:
        matched_match = match_thread_to_match(thread, matches)

        if matched_match:
            match_id = str(matched_match["id"])

            if (
                match_id in existing_matches
                and thread["thread_id"] in existing_matches[match_id]["thread_ids"]
            ):
                skipped_threads += 1
                continue

            if match_id not in match_threads:
                match_threads[match_id] = existing_matches.get(match_id, {}).get(
                    "data",
                    {
                        "match_id": match_id,
                        "match_date": date,
                        "home_team": matched_match["home_team"],
                        "away_team": matched_match["away_team"],
                        "competition": matched_match["competition"],
                        "threads": [],
                    },
                )

            thread_data = {
                "thread_type": thread["flair"],
                "thread_id": thread["thread_id"],
                "title": thread["title"],
                "body": thread["body"],
                "created_utc": thread["created_utc"],
                "score": thread["score"],
                "num_comments": thread["num_comments"],
                "comments": thread["top_comments"],
            }
            match_threads[match_id]["threads"].append(thread_data)
            new_threads_processed += 1
        else:
            skipped_threads += 1

    # Validate and save matches
    for match_id, match_data in match_threads.items():
        validation = validate_match_data(match_data, bucket_name)
        validation_results.append(validation)

        if validation["valid"]:
            match_blob = bucket.blob(f"reddit_data/matches/{match_id}.json")
            match_blob.upload_from_string(
                json.dumps(match_data, indent=2), content_type="application/json"
            )
        else:
            logging.warning(
                f"Match {match_id} failed validation: {validation['failed']}"
            )
            new_threads_processed -= len(match_data["threads"])
            skipped_threads += len(match_data["threads"])

    return {
        "date": date,
        "processed_threads": new_threads_processed,
        "skipped_threads": skipped_threads,
        "validations": validation_results,
    }
