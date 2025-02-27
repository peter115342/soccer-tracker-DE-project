import os
import logging
import requests
from google.cloud import bigquery
from typing import List, Dict, Optional


class MatchValidator:
    """Class to validate match data by comparing BigQuery data with the Football-data.org API."""

    def __init__(self, api_key: Optional[str] = None):
        """Initialize with optional API key."""
        self.api_key = api_key or os.environ.get("FOOTBALL_DATA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Football Data API key not provided or found in environment variables"
            )

        self.headers = {"X-Auth-Token": self.api_key}
        self.api_base_url = "https://api.football-data.org/v4"
        self.bq_client = bigquery.Client()

    def get_matches_from_bq(self, hours: int = 24) -> List[Dict]:
        """Fetch matches from the last N hours from BigQuery."""
        query = f"""
        SELECT 
            id, 
            homeTeam.name as home_team_name,
            awayTeam.name as away_team_name,
            CASE 
                WHEN score.fullTime.homeTeam IS NOT NULL THEN score.fullTime.homeTeam
                ELSE -1
            END as home_score,
            CASE 
                WHEN score.fullTime.awayTeam IS NOT NULL THEN score.fullTime.awayTeam
                ELSE -1
            END as away_score,
            status,
            utcDate,
            lastUpdated,
            competition.id as competition_id
        FROM `sports_data_eu.matches_processed`
        WHERE utcDate >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
        """  # nosec B608

        query_job = self.bq_client.query(query)
        results = query_job.result()

        return [dict(row.items()) for row in results]

    def get_match_from_api(self, match_id: int) -> Optional[Dict]:
        """Fetch a specific match from the Football-data.org API."""
        url = f"{self.api_base_url}/matches/{match_id}"

        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching match {match_id} from API: {str(e)}")
            return None

    def validate_matches(self) -> List[Dict]:
        """
        Validate matches by comparing BQ data with API data.
        Returns a list of discrepancies found.
        """
        discrepancies = []
        bq_matches = self.get_matches_from_bq()

        if not bq_matches:
            logging.info("No matches found in the last 24 hours.")
            return []

        for bq_match in bq_matches:
            match_id = bq_match["id"]

            if bq_match["status"] not in ["FINISHED", "AWARDED"]:
                continue

            api_match = self.get_match_from_api(match_id)

            if not api_match:
                logging.warning(
                    f"Could not fetch match {match_id} from API. Skipping validation."
                )
                continue

            try:
                api_home_score = api_match["score"]["fullTime"]["home"]
                api_away_score = api_match["score"]["fullTime"]["away"]

                bq_home_score = bq_match["home_score"]
                bq_away_score = bq_match["away_score"]

                if (
                    (api_home_score != bq_home_score or api_away_score != bq_away_score)
                    and bq_home_score != -1
                    and bq_away_score != -1
                ):
                    discrepancy = {
                        "match_id": match_id,
                        "home_team": bq_match["home_team_name"],
                        "away_team": bq_match["away_team_name"],
                        "bq_score": f"{bq_home_score}-{bq_away_score}",
                        "api_score": f"{api_home_score}-{api_away_score}",
                        "utc_date": bq_match["utcDate"],
                        "competition_id": bq_match["competition_id"],
                    }
                    discrepancies.append(discrepancy)
            except (KeyError, TypeError) as e:
                logging.error(f"Error comparing match {match_id}: {str(e)}")
                continue

        return discrepancies
