# Pub/Sub Topics
resource "google_pubsub_topic" "fetch_league_data" {
  name    = "fetch_league_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "fetch_reddit_data" {
  name    = "fetch_reddit_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "fetch_football_data" {
  name    = "fetch_football_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "fetch_weather_data" {
  name    = "fetch_weather_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "convert_to_parquet" {
  name    = "convert_to_parquet_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "convert_weather_to_parquet" {
  name    = "convert_weather_to_parquet_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "match_to_bigquery" {
  name    = "match_to_bigquery_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "weather_to_bigquery" {
  name    = "weather_to_bigquery_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "transform_matches" {
  name    = "transform_matches_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "transform_weather" {
  name    = "transform_weather_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "fetch_standings_data" {
  name    = "fetch_standings_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "convert_standings_to_parquet" {
  name    = "convert_standings_to_parquet_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "standings_to_bigquery" {
  name    = "standings_to_bigquery_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "transform_standings" {
  name    = "transform_standings_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "convert_reddit_to_parquet" {
  name    = "convert_reddit_to_parquet_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "reddit_to_bigquery" {
  name    = "reddit_to_bigquery_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "process_reddit_data" {
  name    = "process_reddit_data_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "transform_reddit" {
  name    = "transform_reddit_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "trigger_quality_scans" {
  name    = "trigger_quality_scans_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "sync_matches_to_firestore" {
  name    = "sync_matches_to_firestore_topic"
  project = var.project_id
}

resource "google_pubsub_topic" "sync_standings_to_firestore" {
  name    = "sync_standings_to_firestore_topic"
  project = var.project_id
}

resource "google_pubsub_subscription" "fetch_league_data" {
  name                      = "fetch_league_data_subscription"
  topic                     = google_pubsub_topic.fetch_league_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://fetch-league-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ffetch_league_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://fetch-league-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "fetch_reddit_data" {
  name                      = "fetch_reddit_data_subscription"
  topic                     = google_pubsub_topic.fetch_reddit_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://fetch-reddit-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ffetch_reddit_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://fetch-reddit-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "fetch_football_data" {
  name                      = "fetch_football_data_subscription"
  topic                     = google_pubsub_topic.fetch_football_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://fetch-football-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ffetch_football_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://fetch-football-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "fetch_weather_data" {
  name                      = "fetch_weather_data_subscription"
  topic                     = google_pubsub_topic.fetch_weather_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://fetch-weather-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ffetch_weather_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://fetch-weather-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "convert_to_parquet" {
  name                      = "convert_to_parquet_subscription"
  topic                     = google_pubsub_topic.convert_to_parquet.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-match-to-parquet-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fconvert_to_parquet_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-match-to-parquet-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "convert_weather_to_parquet" {
  name                      = "convert_weather_to_parquet_subscription"
  topic                     = google_pubsub_topic.convert_weather_to_parquet.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-weather-to-parquet-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fconvert_weather_to_parquet_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-weather-to-parquet-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "match_to_bigquery" {
  name                      = "match_to_bigquery_subscription"
  topic                     = google_pubsub_topic.match_to_bigquery.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://load-matches-to-bigquery-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fmatch_to_bigquery_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://load-matches-to-bigquery-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "weather_to_bigquery" {
  name                      = "weather_to_bigquery_subscription"
  topic                     = google_pubsub_topic.weather_to_bigquery.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://load-weather-to-bigquery-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fweather_to_bigquery_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://load-weather-to-bigquery-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "transform_matches" {
  name                      = "transform_matches_subscription"
  topic                     = google_pubsub_topic.transform_matches.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-matches-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ftransform_matches_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-matches-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "transform_weather" {
  name                      = "transform_weather_subscription"
  topic                     = google_pubsub_topic.transform_weather.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-weather-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ftransform_weather_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-weather-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "fetch_standings_data" {
  name                      = "fetch_standings_data_subscription"
  topic                     = google_pubsub_topic.fetch_standings_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://fetch-standings-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ffetch_standings_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://fetch-standings-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "convert_standings_to_parquet" {
  name                      = "convert_standings_to_parquet_subscription"
  topic                     = google_pubsub_topic.convert_standings_to_parquet.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-standings-to-parquet-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fconvert_standings_to_parquet_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-standings-to-parquet-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "standings_to_bigquery" {
  name                      = "standings_to_bigquery_subscription"
  topic                     = google_pubsub_topic.standings_to_bigquery.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://load-standings-to-bigquery-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fstandings_to_bigquery_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://load-standings-to-bigquery-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "transform_standings" {
  name                      = "transform_standings_subscription"
  topic                     = google_pubsub_topic.transform_standings.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-standings-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ftransform_standings_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-standings-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "convert_reddit_to_parquet" {
  name                      = "convert_reddit_to_parquet_subscription"
  topic                     = google_pubsub_topic.convert_reddit_to_parquet.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-reddit-to-parquet-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fconvert_reddit_to_parquet_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-reddit-to-parquet-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "reddit_to_bigquery" {
  name                      = "reddit_to_bigquery_subscription"
  topic                     = google_pubsub_topic.reddit_to_bigquery.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://load-reddit-to-bigquery-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Freddit_to_bigquery_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://load-reddit-to-bigquery-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "process_reddit_data" {
  name                      = "process_reddit_data_subscription"
  topic                     = google_pubsub_topic.process_reddit_data.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://process-reddit-data-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fprocess_reddit_data_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://process-reddit-data-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "transform_reddit" {
  name                      = "transform_reddit_subscription"
  topic                     = google_pubsub_topic.transform_reddit.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://transform-reddit-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ftransform_reddit_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://transform-reddit-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "trigger_quality_scans" {
  name                      = "trigger_quality_scans_subscription"
  topic                     = google_pubsub_topic.trigger_quality_scans.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://trigger-dataplex-scans-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Ftrigger_quality_scans_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://trigger-dataplex-scans-a2h6lbpipq-lm.a.run.app"
    }
  }
}

data "google_pubsub_topic" "trigger_quality_scans" {
  name = "trigger_quality_scans_topic"
}



resource "google_pubsub_subscription" "sync_matches_to_firestore" {
  name                      = "sync_matches_to_firestore_subscription"
  topic                     = google_pubsub_topic.sync_matches_to_firestore.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://sync-matches-to-firestore-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fsync_matches_to_firestore_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://sync-matches-to-firestore-a2h6lbpipq-lm.a.run.app"
    }
  }
}

resource "google_pubsub_subscription" "sync_standings_to_firestore" {
  name                      = "sync_standings_to_firestore_subscription"
  topic                     = google_pubsub_topic.sync_standings_to_firestore.id
  project                   = var.project_id
  message_retention_duration = "2678400s"
  expiration_policy {
    ttl = ""
  }
  retry_policy {
    minimum_backoff = "10s"
  }
  ack_deadline_seconds = 600

  push_config {
    push_endpoint = "https://sync-standings-to-firestore-a2h6lbpipq-lm.a.run.app?__GCP_CloudEventsMode=CUSTOM_PUBSUB_projects%2F${var.project_id}%2Ftopics%2Fsync_standings_to_firestore_topic"
    
    oidc_token {
      service_account_email = var.service_account_email
      audience = "https://sync-standings-to-firestore-a2h6lbpipq-lm.a.run.app"
    }
  }
}