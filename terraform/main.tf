terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.16.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7.0"
    }
  }

  backend "gcs" {

  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "function_bucket" {
  name     = "${var.project_id}-function-source"
  location = var.region
  uniform_bucket_level_access = true
}

data "archive_file" "fetch_league_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/league_data/fetch_league_data_function"
  output_path = "../cloud_functions/league_data/fetch_league_data_function.zip"
}

data "archive_file" "fetch_football_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/match_data/fetch_match_data_function"
  output_path = "../cloud_functions/match_data/fetch_match_data_function.zip"
}

data "archive_file" "fetch_weather_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/weather_data/fetch_weather_data_function"
  output_path = "../cloud_functions/weather_data/fetch_weather_data_function.zip"
}

data "archive_file" "transform_match_to_parquet" {
  type        = "zip"
  source_dir  = "../cloud_functions/match_data/convert_match_data_function"
  output_path = "../cloud_functions/match_data/convert_match_data_function.zip"
}

data "archive_file" "transform_weather_to_parquet" {
  type        = "zip"
  source_dir  = "../cloud_functions/weather_data/convert_weather_data_function"
  output_path = "../cloud_functions/weather_data/convert_weather_data_function.zip"
}

data "archive_file" "load_matches_to_bigquery" {
  type        = "zip"
  source_dir  = "../cloud_functions/match_data/match_to_bigquery_function"
  output_path = "../cloud_functions/match_data/match_to_bigquery_function.zip"
}

data "archive_file" "load_weather_to_bigquery" {
  type        = "zip"
  source_dir  = "../cloud_functions/weather_data/weather_to_bigquery_function"
  output_path = "../cloud_functions/weather_data/weather_to_bigquery_function.zip"
}

data "archive_file" "transform_matches" {
  type        = "zip"
  source_dir  = "../cloud_functions/match_data/match_transform_function"
  output_path = "../cloud_functions/match_data/match_transform_function.zip"
}

data "archive_file" "transform_weather" {
  type        = "zip"
  source_dir  = "../cloud_functions/weather_data/weather_transform_function"
  output_path = "../cloud_functions/weather_data/weather_transform_function.zip"
}

data "archive_file" "fetch_standings_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/standings_data/fetch_standings_data_function"
  output_path = "../cloud_functions/standings_data/fetch_standings_data_function.zip"
}

data "archive_file" "transform_standings_to_parquet" {
  type        = "zip"
  source_dir  = "../cloud_functions/standings_data/convert_standings_data_function"
  output_path = "../cloud_functions/standings_data/convert_standings_data_function.zip"
}

data "archive_file" "load_standings_to_bigquery" {
  type        = "zip"
  source_dir  = "../cloud_functions/standings_data/standings_to_bigquery_function"
  output_path = "../cloud_functions/standings_data/standings_to_bigquery_function.zip"
}

data "archive_file" "transform_standings" {
  type        = "zip"
  source_dir  = "../cloud_functions/standings_data/standings_transform_function"
  output_path = "../cloud_functions/standings_data/standings_transform_function.zip"
}

data "archive_file" "fetch_reddit_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/reddit_data/fetch_reddit_data_function"
  output_path = "../cloud_functions/reddit_data/fetch_reddit_data_function.zip"
}

data "archive_file" "transform_reddit_to_parquet" {
  type        = "zip"
  source_dir  = "../cloud_functions/reddit_data/convert_reddit_data_function"
  output_path = "../cloud_functions/reddit_data/convert_reddit_data_function.zip"
}

data "archive_file" "load_reddit_to_bigquery" {
  type        = "zip"
  source_dir  = "../cloud_functions/reddit_data/reddit_to_bigquery_function"
  output_path = "../cloud_functions/reddit_data/reddit_to_bigquery_function.zip"
}

data "archive_file" "process_reddit_data" {
  type        = "zip"
  source_dir  = "../cloud_functions/reddit_data/process_reddit_data_function"
  output_path = "../cloud_functions/reddit_data/process_reddit_data_function.zip"
}

data "archive_file" "transform_reddit" {
  type        = "zip"
  source_dir  = "../cloud_functions/reddit_data/reddit_transform_function"
  output_path = "../cloud_functions/reddit_data/reddit_transform_function.zip"
}