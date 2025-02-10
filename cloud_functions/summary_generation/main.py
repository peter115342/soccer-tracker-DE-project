import os
import json
import logging
import requests

from google.cloud import bigquery, storage
from google import genai
from google.genai import types


def generate_match_summary(event, context):
    """
    Cloud Function to generate a markdown summary article per league per match_date for which matches have Reddit data.
    Only considers matches (from matches_processed) that have any Reddit data (reddit_processed join)
    and at least one Reddit thread with score >= 40.
    Uses Gemini 2.0 Flash via Google Gen AI SDK to generate the final article text.
    The generated markdown documents are then saved to GCS in the bucket specified by the BUCKET_NAME env var,
    under the folder "match_summaries" with filename format "{match_date}_{league}.md".
    No duplicate file is generated; if a file with the same name exists, generation for that date/league is skipped.
    """
    try:
        sql_query = f"""
WITH matches_with_reddit AS (
  SELECT 
    m.id,
    m.utcDate,
    m.status,
    m.matchday,
    m.stage,
    m.homeTeam,
    m.awayTeam,
    m.competition,
    r.threads
  FROM `{os.environ.get("GCP_PROJECT_ID")}.your_dataset.matches_processed` AS m
  JOIN `{os.environ.get("GCP_PROJECT_ID")}.your_dataset.reddit_processed` AS r
    ON CAST(m.id AS STRING) = r.match_id
),
filtered_matches AS (
  SELECT *
  FROM matches_with_reddit
  WHERE EXISTS (
      SELECT 1 FROM UNNEST(threads) as thread WHERE thread.score >= 40
  )
)
SELECT 
  DATE(utcDate) AS match_date,
  competition.name AS league,
  ARRAY_AGG(STRUCT(
    id,
    status,
    matchday,
    stage,
    homeTeam,
    awayTeam,
    threads
  )) AS matches
FROM filtered_matches
GROUP BY match_date, league
ORDER BY match_date, league
"""  # nosec B608
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
            match_date = row.match_date
            league = row.league
            prompt = f"# {league} Match Summary - {match_date}\n\n"
            prompt += "## Matches\n\n"
            for match in row.matches:
                home_team = (
                    match.homeTeam.name
                    if match.homeTeam and "name" in match.homeTeam
                    else "N/A"
                )
                away_team = (
                    match.awayTeam.name
                    if match.awayTeam and "name" in match.awayTeam
                    else "N/A"
                )
                status = match.status if match.status else "N/A"
                matchday = match.matchday if match.matchday is not None else "N/A"
                prompt += (
                    f"- **Match ID:** {match.id}, **Status:** {status}, "
                    f"**Matchday:** {matchday}, **Home:** {home_team} vs **Away:** {away_team}\n"
                )
            prompt += "\n## Reddit Highlights\n\n"
            for match in row.matches:
                threads = match.threads
                for thread in threads:
                    if thread.score >= 40:
                        prompt += f"- {thread.title}\n"
            prompt += "\nPlease generate a detailed article summarizing these matches in markdown."
            summaries.append((match_date, league, prompt))

        genai_client = genai.Client(
            vertexai=True,
            project=os.environ.get("GCP_PROJECT_ID"),
            location="us-central1",
        )
        model = "gemini-2.0-flash-001"
        generated_count = 0
        for match_date, league, prompt in summaries:
            filename = f"match_summaries/{match_date}_{league}.md"
            blob = bucket.blob(filename)
            if blob.exists(storage_client):
                logging.info(
                    f"File {filename} already exists. Skipping generation for this date/league combination."
                )
                continue

            contents = [types.Content(role="user", parts=[{"text": prompt}])]
            generate_content_config = types.GenerateContentConfig(
                temperature=1,
                top_p=0.95,
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
                article_text += chunk.text

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

        success_message = f"Successfully generated and saved match summaries for {generated_count} league/date groups."
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
    """
    Save the given content as a file in the GCS bucket specified by the BUCKET_NAME env var,
    under the given filename.
    """
    blob = bucket.blob(filename)
    blob.upload_from_string(content, content_type="text/markdown")


def send_discord_notification(title: str, message: str, color: int):
    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        logging.warning("Discord webhook URL not set.")
        return
    discord_data = {
        "content": None,
        "embeds": [
            {
                "title": title,
                "description": message,
                "color": color,
            }
        ],
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(
        webhook_url, data=json.dumps(discord_data), headers=headers, timeout=90
    )
    if response.status_code != 204:
        logging.error(
            f"Failed to send Discord notification: {response.status_code}, {response.text}"
        )
