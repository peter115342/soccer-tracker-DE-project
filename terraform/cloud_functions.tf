resource "google_storage_bucket" "function_bucket" {
  name     = var.functions_bucket_name
  location = "europe-central2"
  uniform_bucket_level_access = true
}



resource "google_cloudfunctions2_function" "trigger_dataplex_scans" {
  name        = "trigger_dataplex_scans"
  location    = "europe-central2"
  description = "Function to trigger Dataplex quality scans"

  build_config {
    runtime     = "python312"
    entry_point = "trigger_dataplex_scans"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/data_validation/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    ingress_settings = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.trigger_quality_scans.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "fetch_league_data" {
  name        = "fetch_league_data"
  location    = "europe-central2"
  description = "Function to fetch league data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_league_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/league_data/fetch_league_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GOOGLE_MAPS_API_KEY = var.google_maps_api_key
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_league_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "fetch_football_data" {
  name        = "fetch_football_data"
  location    = "europe-central2"
  description = "Function to fetch football match data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_football_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/match_data/fetch_match_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
      BUCKET_NAME        = var.bucket_name
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_football_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "fetch_weather_data" {
  name        = "fetch_weather_data"
  location    = "europe-central2"
  description = "Function to fetch weather data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_weather_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/weather_data/fetch_weather_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_weather_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_match_to_parquet" {
  name        = "transform_match_to_parquet"
  location    = "europe-central2"
  description = "Function to transform match data to parquet"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/match_data/convert_match_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_to_parquet.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_weather_to_parquet" {
  name        = "transform_weather_to_parquet"
  location    = "europe-central2"
  description = "Function to transform weather data to parquet"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/weather_data/convert_weather_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_weather_to_parquet.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "load_matches_to_bigquery" {
  name        = "load_matches_to_bigquery"
  location    = "europe-central2"
  description = "Function to load matches to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_matches_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/match_data/match_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.match_to_bigquery.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "load_weather_to_bigquery" {
  name        = "load_weather_to_bigquery"
  location    = "europe-central2"
  description = "Function to load weather data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_weather_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/weather_data/weather_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.weather_to_bigquery.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_matches" {
  name        = "transform_matches"
  location    = "europe-central2"
  description = "Function to transform matches"

  build_config {
    runtime     = "python312"
    entry_point = "transform_matches"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/match_data/match_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_matches.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_weather" {
  name        = "transform_weather"
  location    = "europe-central2"
  description = "Function to transform weather data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_weather"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/weather_data/weather_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_weather.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "fetch_standings_data" {
  name        = "fetch_standings_data"
  location    = "europe-central2"
  description = "Function to fetch standings data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_standings_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/standings_data/fetch_standings_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
      BUCKET_NAME        = var.bucket_name
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_standings_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_standings_to_parquet" {
  name        = "transform_standings_to_parquet"
  location    = "europe-central2"
  description = "Function to transform standings to parquet"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/standings_data/convert_standings_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_standings_to_parquet.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "load_standings_to_bigquery" {
  name        = "load_standings_to_bigquery"
  location    = "europe-central2"
  description = "Function to load standings to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_standings_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/standings_data/standings_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.standings_to_bigquery.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_standings" {
  name        = "transform_standings"
  location    = "europe-central2"
  description = "Function to transform standings"

  build_config {
    runtime     = "python312"
    entry_point = "transform_standings"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/standings_data/standings_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_standings.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "fetch_reddit_data" {
  name        = "fetch_reddit_data"
  location    = "europe-central2"
  description = "Function to fetch reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_reddit_data"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/reddit_data/fetch_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      REDDIT_CLIENT_ID     = var.reddit_client_id
      REDDIT_CLIENT_SECRET = var.reddit_client_secret
      DISCORD_WEBHOOK_URL  = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.fetch_reddit_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_reddit_to_parquet" {
  name        = "transform_reddit_to_parquet"
  location    = "europe-central2"
  description = "Function to transform reddit data to parquet"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/reddit_data/convert_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.convert_reddit_to_parquet.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "load_reddit_to_bigquery" {
  name        = "load_reddit_to_bigquery"
  location    = "europe-central2"
  description = "Function to load reddit data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_reddit_to_bigquery"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/reddit_data/reddit_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.reddit_to_bigquery.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "process_reddit_data" {
  name        = "process_reddit_data"
  location    = "europe-central2"
  description = "Function to process reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "process_reddit_threads"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/reddit_data/process_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.process_reddit_data.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "transform_reddit" {
  name        = "transform_reddit"
  location    = "europe-central2"
  description = "Function to transform reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_reddit"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/reddit_data/reddit_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email

    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.transform_reddit.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}

resource "google_cloudfunctions2_function" "sync_matches_to_firestore" {
  name        = "sync_matches_to_firestore"
  location    = "europe-central2"
  description = "Function to sync matches to Firestore"

  build_config {
    runtime     = "python312"
    entry_point = "sync_matches_to_firestore"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/serving_layer/sync_to_firestore_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.sync_matches_to_firestore.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}


resource "google_cloudfunctions2_function" "sync_standings_to_firestore" {
  name        = "sync_standings_to_firestore"
  location    = "europe-central2"
  description = "Function to sync current standings to Firestore"

  build_config {
    runtime     = "python312"
    entry_point = "sync_standings_to_firestore"
    source {
      storage_source {
        bucket = google_storage_bucket.function_bucket.name
        object = "cloud_functions/serving_layer/sync_standings_to_firestore_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    service_account_email = var.service_account_email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
    }
  }

  event_trigger {
    trigger_region = "europe-central2"
    event_type     = "google.cloud.pubsub.topic.v1.messagePublished"
    pubsub_topic   = google_pubsub_topic.sync_standings_to_firestore.id
    retry_policy   = "RETRY_POLICY_RETRY"
  }
}