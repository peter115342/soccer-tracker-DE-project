# Define Pub/Sub Topics
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

# League Data Function
resource "google_cloudfunctions2_function" "fetch_league_data" {
  name     = "fetch_league_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "fetch_league_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.fetch_league_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GOOGLE_MAPS_API_KEY = var.google_maps_api_key
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_league_data.id
  }
}

# Football Data Function
resource "google_cloudfunctions2_function" "fetch_football_data" {
  name     = "fetch_football_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "fetch_football_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.fetch_football_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID      = var.project_id
      BUCKET_NAME         = var.bucket_name
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_football_data.id
  }
}

# Weather Data Function
resource "google_cloudfunctions2_function" "fetch_weather_data" {
  name     = "fetch_weather_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "fetch_weather_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.fetch_weather_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_weather_data.id
  }
}

# Match Conversion Function
resource "google_cloudfunctions2_function" "transform_match_to_parquet" {
  name     = "transform_match_to_parquet"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_match_to_parquet.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_to_parquet.id
  }
}

# Weather Conversion Function
resource "google_cloudfunctions2_function" "transform_weather_to_parquet" {
  name     = "transform_weather_to_parquet"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_weather_to_parquet.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_weather_to_parquet.id
  }
}

# Match to BigQuery Function
resource "google_cloudfunctions2_function" "load_matches_to_bigquery" {
  name     = "load_matches_to_bigquery"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "load_matches_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.load_matches_to_bigquery.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.match_to_bigquery.id
  }
}

# Weather to BigQuery Function
resource "google_cloudfunctions2_function" "load_weather_to_bigquery" {
  name     = "load_weather_to_bigquery"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "load_weather_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.load_weather_to_bigquery.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.weather_to_bigquery.id
  }
}

# Match Transform Function
resource "google_cloudfunctions2_function" "transform_matches" {
  name     = "transform_matches"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_matches"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_matches.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_matches.id
  }
}

# Weather Transform Function
resource "google_cloudfunctions2_function" "transform_weather" {
  name     = "transform_weather"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_weather"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_weather.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_weather.id
  }
}

# Standings Data Function
resource "google_cloudfunctions2_function" "fetch_standings_data" {
  name     = "fetch_standings_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "fetch_standings_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.fetch_standings_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID      = var.project_id
      BUCKET_NAME         = var.bucket_name
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_standings_data.id
  }
}

# Standings Data Conversion Function
resource "google_cloudfunctions2_function" "transform_standings_to_parquet" {
  name     = "transform_standings_to_parquet"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_standings_to_parquet.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_standings_to_parquet.id
  }
}

# Standings to BigQuery Function
resource "google_cloudfunctions2_function" "load_standings_to_bigquery" {
  name     = "load_standings_to_bigquery"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "load_standings_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.load_standings_to_bigquery.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.standings_to_bigquery.id
  }
}

# Standings Transform Function
resource "google_cloudfunctions2_function" "transform_standings" {
  name     = "transform_standings"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_standings"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_standings.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL  = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_standings.id
  }
}

# Reddit Data Function
resource "google_cloudfunctions2_function" "fetch_reddit_data" {
  name     = "fetch_reddit_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "fetch_reddit_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.fetch_reddit_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      REDDIT_CLIENT_ID     = var.reddit_client_id
      REDDIT_CLIENT_SECRET = var.reddit_client_secret
      DISCORD_WEBHOOK_URL  = var.discord_webhook_url
      BUCKET_NAME          = var.bucket_name
      GCP_PROJECT_ID       = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_reddit_data.id
  }
}

# Reddit Data Conversion Function
resource "google_cloudfunctions2_function" "transform_reddit_to_parquet" {
  name     = "transform_reddit_to_parquet"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_reddit_to_parquet.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_reddit_to_parquet.id
  }
}

# Reddit to BigQuery Function
resource "google_cloudfunctions2_function" "load_reddit_to_bigquery" {
  name     = "load_reddit_to_bigquery"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "load_reddit_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.load_reddit_to_bigquery.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.reddit_to_bigquery.id
  }
}

# Process Reddit Data Function
resource "google_cloudfunctions2_function" "process_reddit_data" {
  name     = "process_reddit_data"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "process_reddit_threads"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.process_reddit_data.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.process_reddit_data.id
  }
}

# Reddit Transform Function
resource "google_cloudfunctions2_function" "transform_reddit" {
  name     = "transform_reddit"
  location = var.region

  build_config {
    runtime     = "python312"
    entry_point = "transform_reddit"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = google_storage_bucket_object.transform_reddit.name
      }
    }
  }

  service_config {
    max_instance_count    = 1
    available_memory      = "1024M"
    timeout_seconds       = 540
    service_account_email = data.google_compute_default_service_account.default.email
    environment_variables = {
      DISCORD_WEBHOOK_URL  = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = var.region
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_reddit.id
  }
}
