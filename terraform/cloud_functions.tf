# First, create a service account for the functions
resource "google_service_account" "function_sa" {
  account_id   = "cloud-functions-sa"
  display_name = "Cloud Functions Service Account"
}

# League Data Function
resource "google_storage_bucket_object" "fetch_league_data" {
  name   = "functions/fetch_league_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.fetch_league_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GOOGLE_MAPS_API_KEY = var.google_maps_api_key
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Football Data Function
resource "google_storage_bucket_object" "fetch_football_data" {
  name   = "functions/fetch_football_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.fetch_football_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID      = var.project_id
      BUCKET_NAME         = var.bucket_name
    }
  }
}

# Weather Data Function
resource "google_storage_bucket_object" "fetch_weather_data" {
  name   = "functions/fetch_weather_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.fetch_weather_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Match Data Conversion Function
resource "google_storage_bucket_object" "transform_match_to_parquet" {
  name   = "functions/transform_match_to_parquet.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_match_to_parquet.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Weather Data Conversion Function
resource "google_storage_bucket_object" "transform_weather_to_parquet" {
  name   = "functions/transform_weather_to_parquet.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_weather_to_parquet.output_path
}

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
}

# Match to BigQuery Function
resource "google_storage_bucket_object" "load_matches_to_bigquery" {
  name   = "functions/load_matches_to_bigquery.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.load_matches_to_bigquery.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Weather to BigQuery Function
resource "google_storage_bucket_object" "load_weather_to_bigquery" {
  name   = "functions/load_weather_to_bigquery.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.load_weather_to_bigquery.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Match Transform Function
resource "google_storage_bucket_object" "transform_matches" {
  name   = "functions/transform_matches.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_matches.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }
}

# Weather Transform Function
resource "google_storage_bucket_object" "transform_weather" {
  name   = "functions/transform_weather.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_weather.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }
}

# Standings Data Function
resource "google_storage_bucket_object" "fetch_standings_data" {
  name   = "functions/fetch_standings_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.fetch_standings_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID      = var.project_id
      BUCKET_NAME         = var.bucket_name
    }
  }
}

# Standings Data Conversion Function
resource "google_storage_bucket_object" "transform_standings_to_parquet" {
  name   = "functions/transform_standings_to_parquet.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_standings_to_parquet.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Standings to BigQuery Function
resource "google_storage_bucket_object" "load_standings_to_bigquery" {
  name   = "functions/load_standings_to_bigquery.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.load_standings_to_bigquery.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Standings Transform Function
resource "google_storage_bucket_object" "transform_standings" {
  name   = "functions/transform_standings.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_standings.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }
}

# Reddit Data Function
resource "google_storage_bucket_object" "fetch_reddit_data" {
  name   = "functions/fetch_reddit_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.fetch_reddit_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      REDDIT_CLIENT_ID      = var.reddit_client_id
      REDDIT_CLIENT_SECRET  = var.reddit_client_secret
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      BUCKET_NAME          = var.bucket_name
      GCP_PROJECT_ID       = var.project_id
    }
  }
}

# Reddit Data Conversion Function
resource "google_storage_bucket_object" "transform_reddit_to_parquet" {
  name   = "functions/transform_reddit_to_parquet.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_reddit_to_parquet.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Reddit to BigQuery Function
resource "google_storage_bucket_object" "load_reddit_to_bigquery" {
  name   = "functions/load_reddit_to_bigquery.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.load_reddit_to_bigquery.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Process Reddit Data Function
resource "google_storage_bucket_object" "process_reddit_data" {
  name   = "functions/process_reddit_data.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.process_reddit_data.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

# Reddit Transform Function
resource "google_storage_bucket_object" "transform_reddit" {
  name   = "functions/transform_reddit.zip"
  bucket = google_storage_bucket.function_bucket.name
  source = data.archive_file.transform_reddit.output_path
}

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
    service_account_email = google_service_account.function_sa.email
    environment_variables = {
      DISCORD_WEBHOOK_URL    = var.discord_webhook_url
      GCP_PROJECT_ID        = var.project_id
      DATAFORM_REPOSITORY   = var.dataform_repository
      DATAFORM_WORKSPACE    = var.dataform_workspace
    }
  }
}
