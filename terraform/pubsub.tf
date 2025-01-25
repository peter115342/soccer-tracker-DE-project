resource "google_pubsub_topic" "fetch_league_data" {
  name = "fetch_league_data_topic"
}

resource "google_pubsub_topic" "fetch_football_data" {
  name = "fetch_football_data_topic"
}

resource "google_pubsub_topic" "fetch_weather_data" {
  name = "fetch_weather_data_topic"
}

resource "google_pubsub_topic" "convert_to_parquet" {
  name = "convert_to_parquet_topic"
}

resource "google_pubsub_topic" "convert_weather_to_parquet" {
  name = "convert_weather_to_parquet_topic"
}

resource "google_pubsub_topic" "match_to_bigquery" {
  name = "match_to_bigquery_topic"
}

resource "google_pubsub_topic" "weather_to_bigquery" {
  name = "weather_to_bigquery_topic"
}

resource "google_pubsub_topic" "transform_matches" {
  name = "transform_matches_topic"
}

resource "google_pubsub_topic" "transform_weather" {
  name = "transform_weather_topic"
}

resource "google_pubsub_topic" "fetch_standings_data" {
  name = "fetch_standings_data_topic"
}

resource "google_pubsub_topic" "convert_standings_to_parquet" {
  name = "convert_standings_to_parquet_topic"
}

resource "google_pubsub_topic" "standings_to_bigquery" {
  name = "standings_to_bigquery_topic"
}

resource "google_pubsub_topic" "transform_standings" {
  name = "transform_standings_topic"
}

resource "google_pubsub_topic" "fetch_reddit_data" {
  name = "fetch_reddit_data_topic"
}

resource "google_pubsub_topic" "convert_reddit_to_parquet" {
  name = "convert_reddit_to_parquet_topic"
}

resource "google_pubsub_topic" "reddit_to_bigquery" {
  name = "reddit_to_bigquery_topic"
}

resource "google_pubsub_topic" "process_reddit_data" {
  name = "process_reddit_data_topic"
}

resource "google_pubsub_topic" "transform_reddit" {
  name = "transform_reddit_topic"
}