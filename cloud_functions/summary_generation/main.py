import os
import json
import logging
import base64
import unicodedata
from google.cloud import bigquery, storage
from google import genai
from google.genai import types
from pydantic import BaseModel
from typing import List
from cloud_functions.discord_utils.discord_notifications import (
    send_discord_notification,
)


def normalize_text(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


class MatchSummaryPrompt(BaseModel):
    match_date: str
    league: str
    matches: List[dict]

    def generate_prompt(self) -> str:
        prompt = f"""
Generate a narrative match summary article for {normalize_text(self.league)} matches on {self.match_date}.

Write engaging match summaries that:
1. Start with a clear headline and match result
2. Write 2-3 concise paragraphs that:
   - Describe the match result and significance
   - Mention weather conditions if they impacted the game
   - Include both teams' current form for context
   - Incorporate relevant fan reactions from Reddit
3. Maintain a journalistic style
4. Use only factual information provided

Example format:
# {normalize_text(self.league)} Match Summary - {self.match_date}

## [Home Team] vs [Away Team]
[2-3 paragraphs incorporating all available data into a flowing narrative about the match, weather impact, team forms, and fan reactions. Focus on telling the story of what happened.]

[Continue for each match...]
"""
        for match in self.matches:
            home_team = normalize_text(match["homeTeam"]["name"])
            away_team = normalize_text(match["awayTeam"]["name"])

            score_info = ""
            if match["score"] and match["score"]["fullTime"]:
                home_score = match["score"]["fullTime"]["homeTeam"]
                away_score = match["score"]["fullTime"]["awayTeam"]
                score_info = f"Final Score: {home_score}-{away_score}"

            weather = ""
            if match["temperature_2m"] is not None:
                weather = f"\nWeather: {match['temperature_2m']} degrees C, "
                if match["precipitation"] > 0:
                    weather += f"Precipitation: {match['precipitation']}mm, "
                weather += f"Wind: {match['windspeed_10m']} km/h"

            form_info = ""
            if match["home_team_form"]:
                form_info += f"\nHome team form: {match['home_team_form']}"
            if match["away_team_form"]:
                form_info += f"\nAway team form: {match['away_team_form']}"

            prompt += (
                f"\n{home_team} vs {away_team}\n{score_info}{weather}{form_info}\n"
            )

            if match["threads"]:
                prompt += "\nRelevant Reddit Discussion:\n"
                for thread in match["threads"]:
                    thread_title = normalize_text(thread["title"])
                    prompt += f"- Thread: {thread_title} (Score: {thread['score']})\n"
                    if thread["body"]:
                        thread_body = normalize_text(thread["body"][:200])
                        prompt += f"  Content: {thread_body}...\n"
                    for comment in thread["comments"]:
                        comment_body = normalize_text(comment["body"][:200])
                        prompt += f"  Comment: {comment_body}... (Score: {comment['score']})\n"

        prompt += """
Please generate a comprehensive match summary using only the provided information above.
Focus on factual information and avoid any speculation or assumptions."""

        return prompt


def generate_match_summary(event, context):
    """Cloud Function to generate factual match summaries using available match, weather, standings and Reddit data"""
    try:
        pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
        message_data = json.loads(pubsub_message)

        if message_data.get("action") != "generate_match_summary":
            error_message = "Invalid message format"
            logging.error(error_message)
            send_discord_notification(
                "❌ Match Summary Generation: Invalid Trigger", error_message, 16711680
            )
            return error_message, 500

        sql_query = """
WITH matches_with_data AS (
  SELECT 
    m.id,
    m.utcDate,
    m.status,
    m.matchday,
    m.stage,
    m.homeTeam,
    m.awayTeam,
    m.competition,
    m.score,
    r.threads,
    w.temperature_2m,
    w.precipitation,
    w.weathercode,
    w.windspeed_10m
  FROM `sports_data_eu.matches_processed` AS m
  JOIN `sports_data_eu.reddit_processed` AS r
    ON CAST(m.id AS STRING) = r.match_id
  LEFT JOIN `sports_data_eu.weather_processed` w 
    ON m.id = w.match_id
),
team_standings AS (
  SELECT 
    s.fetchDate,
    s.competitionId,
    st.teamId,
    st.form,
    st.position
  FROM `sports_data_eu.standings_processed` s,
    UNNEST(standings) st
  WHERE s.standingType = 'TOTAL'
)
SELECT 
  DATE(m.utcDate) AS match_date,
  m.competition.name AS league,
  ARRAY_AGG(STRUCT(
    m.id,
    m.status,
    m.matchday,
    m.stage,
    m.homeTeam,
    m.awayTeam,
    m.score,
    m.threads,
    m.temperature_2m,
    m.precipitation,
    m.weathercode,
    m.windspeed_10m,
    home_form.form as home_team_form,
    away_form.form as away_team_form
  )) AS matches
FROM matches_with_data m
LEFT JOIN (
  SELECT teamId, form, fetchDate
  FROM team_standings ts
  WHERE fetchDate = (
    SELECT MAX(fetchDate)
    FROM team_standings
    WHERE fetchDate < ts.fetchDate
  )
) home_form
  ON m.homeTeam.id = home_form.teamId 
  AND DATE(m.utcDate) > DATE(home_form.fetchDate)
LEFT JOIN (
  SELECT teamId, form, fetchDate
  FROM team_standings ts
  WHERE fetchDate = (
    SELECT MAX(fetchDate)
    FROM team_standings
    WHERE fetchDate < ts.fetchDate
  )
) away_form
  ON m.awayTeam.id = away_form.teamId
  AND DATE(m.utcDate) > DATE(away_form.fetchDate)
GROUP BY match_date, league
ORDER BY match_date, league
"""
        bq_client = bigquery.Client()
        query_job = bq_client.query(sql_query)
        results = list(query_job.result())

        if not results:
            message = "No eligible matches found across all dates. Skipping generation."
            logging.info(message)
            send_discord_notification(
                "📝 Generate Match Summary: No Data", message, 16776960
            )
            return message, 200

        storage_client = storage.Client()
        bucket_name = os.environ.get("BUCKET_NAME")
        if not bucket_name:
            raise ValueError("BUCKET_NAME environment variable not set.")
        bucket = storage_client.bucket(bucket_name)

        summaries = []
        for row in results:
            summary_data = MatchSummaryPrompt(
                match_date=str(row.match_date),
                league=normalize_text(row.league),
                matches=row.matches,
            )
            prompt = summary_data.generate_prompt()
            summaries.append((row.match_date, row.league, prompt))

        genai_client = genai.Client(
            vertexai=True,
            project=os.environ.get("GCP_PROJECT_ID"),
            location="us-central1",
        )
        model = "gemini-2.0-flash-001"
        generated_count = 0
        for match_date, league, prompt in summaries:
            filename = f"match_summaries/{match_date}_{normalize_text(league)}.md"
            blob = bucket.blob(filename)
            if blob.exists(storage_client):
                logging.info(
                    f"File {filename} already exists. Skipping generation for this date/league combination."
                )
                continue

            contents = [types.Content(role="user", parts=[{"text": prompt}])]
            generate_content_config = types.GenerateContentConfig(
                temperature=0.3,
                top_p=0.8,
                max_output_tokens=2048,
                response_modalities=["TEXT"],
                safety_settings=[
                    types.SafetySetting(
                        category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"
                    ),
                    types.SafetySetting(
                        category="HARM_CATEGORY_HARASSMENT", threshold="OFF"
                    ),
                ],
            )
            article_text = ""
            for chunk in genai_client.models.generate_content_stream(
                model=model,
                contents=contents,
                config=generate_content_config,
            ):
                cleaned_text = unicodedata.normalize("NFKD", chunk.text)
                article_text += cleaned_text.encode("ascii", "ignore").decode("ascii")

            save_to_gcs(article_text, filename, storage_client, bucket)
            logging.info(f"Saved markdown document to GCS with filename: {filename}")
            generated_count += 1

        if generated_count == 0:
            message = "No new match summaries were generated. All date/league groups already exist."
            logging.info(message)
            send_discord_notification(
                "📝 Generate Match Summaries: No New Data", message, 16776960
            )
            return message, 200

        success_message = f"Generated {generated_count} match summaries"
        logging.info(success_message)
        send_discord_notification(
            "✅ Generate Match Summaries: Success", success_message, 65280
        )
        return success_message, 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        logging.exception(error_message)
        send_discord_notification(
            "❌ Generate Match Summaries: Failure", error_message, 16711680
        )
        return error_message, 500


def save_to_gcs(content, filename, storage_client, bucket):
    """Save content to GCS bucket with cleaned text"""
    normalized_content = unicodedata.normalize("NFKD", content)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        normalized_content, content_type="text/markdown; charset=utf-8"
    )
