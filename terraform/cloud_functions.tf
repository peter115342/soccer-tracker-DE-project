resource "google_cloudfunctions2_function" "trigger_dataplex_scans" {
  name        = "trigger_dataplex_scans"
  location    = var.region
  description = "Triggers Dataplex quality scans"

  build_config {
    runtime     = "python312"
    entry_point = "trigger_dataplex_scans"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/data_validation/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "fetch_league_data" {
  name        = "fetch_league_data"
  location    = var.region
  description = "Fetches league data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_league_data"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/league_data/fetch_league_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GOOGLE_MAPS_API_KEY = var.google_maps_api_key
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "fetch_football_data" {
  name        = "fetch_football_data"
  location    = var.region
  description = "Fetches football match data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_football_data"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/match_data/fetch_match_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
      BUCKET_NAME        = var.bucket_name
    }
  }
}

resource "google_cloudfunctions2_function" "fetch_weather_data" {
  name        = "fetch_weather_data"
  location    = var.region
  description = "Fetches weather data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_weather_data"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/weather_data/fetch_weather_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_match_to_parquet" {
  name        = "transform_match_to_parquet"
  location    = var.region
  description = "Converts match data to parquet format"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/match_data/convert_match_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_weather_to_parquet" {
  name        = "transform_weather_to_parquet"
  location    = var.region
  description = "Converts weather data to parquet format"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/weather_data/convert_weather_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "load_matches_to_bigquery" {
  name        = "load_matches_to_bigquery"
  location    = var.region
  description = "Loads match data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_matches_to_bigquery"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/match_data/match_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "load_weather_to_bigquery" {
  name        = "load_weather_to_bigquery"
  location    = var.region
  description = "Loads weather data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_weather_to_bigquery"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/weather_data/weather_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_matches" {
  name        = "transform_matches"
  location    = var.region
  description = "Transforms match data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_matches"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/match_data/match_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }
}

resource "google_cloudfunctions2_function" "transform_weather" {
  name        = "transform_weather"
  location    = var.region
  description = "Transforms weather data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_weather"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/weather_data/weather_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }
}

resource "google_cloudfunctions2_function" "fetch_standings_data" {
  name        = "fetch_standings_data"
  location    = var.region
  description = "Fetches standings data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_standings_data"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/standings_data/fetch_standings_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      API_FOOTBALL_KEY    = var.api_football_key
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      GCP_PROJECT_ID     = var.project_id
      BUCKET_NAME        = var.bucket_name
    }
  }
}

resource "google_cloudfunctions2_function" "transform_standings_to_parquet" {
  name        = "transform_standings_to_parquet"
  location    = var.region
  description = "Converts standings data to parquet format"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/standings_data/convert_standings_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "load_standings_to_bigquery" {
  name        = "load_standings_to_bigquery"
  location    = var.region
  description = "Loads standings data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_standings_to_bigquery"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/standings_data/standings_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_standings" {
  name        = "transform_standings"
  location    = var.region
  description = "Transforms standings data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_standings"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/standings_data/standings_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }
}

resource "google_cloudfunctions2_function" "fetch_reddit_data" {
  name        = "fetch_reddit_data"
  location    = var.region
  description = "Fetches Reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "fetch_reddit_data"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/reddit_data/fetch_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      REDDIT_CLIENT_ID     = var.reddit_client_id
      REDDIT_CLIENT_SECRET = var.reddit_client_secret
      DISCORD_WEBHOOK_URL  = var.discord_webhook_url
      BUCKET_NAME         = var.bucket_name
      GCP_PROJECT_ID      = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_reddit_to_parquet" {
  name        = "transform_reddit_to_parquet"
  location    = var.region
  description = "Converts Reddit data to parquet format"

  build_config {
    runtime     = "python312"
    entry_point = "transform_to_parquet"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/reddit_data/convert_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "load_reddit_to_bigquery" {
  name        = "load_reddit_to_bigquery"
  location    = var.region
  description = "Loads Reddit data to BigQuery"

  build_config {
    runtime     = "python312"
    entry_point = "load_reddit_to_bigquery"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/reddit_data/reddit_to_bigquery_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "process_reddit_data" {
  name        = "process_reddit_data"
  location    = var.region
  description = "Processes Reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "process_reddit_threads"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/reddit_data/process_reddit_data_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL = var.discord_webhook_url
      BUCKET_NAME        = var.bucket_name
      GCP_PROJECT_ID     = var.project_id
    }
  }
}

resource "google_cloudfunctions2_function" "transform_reddit" {
  name        = "transform_reddit"
  location    = var.region
  description = "Transforms Reddit data"

  build_config {
    runtime     = "python312"
    entry_point = "transform_reddit"
    source {
      storage_source {
        bucket = var.bucket_name
        object = "cloud_functions/reddit_data/reddit_transform_function/source.zip"
      }
    }
  }

  service_config {
    max_instance_count = 1
    available_memory   = "1024M"
    timeout_seconds    = 540
    environment_variables = {
      DISCORD_WEBHOOK_URL   = var.discord_webhook_url
      GCP_PROJECT_ID       = var.project_id
      DATAFORM_REPOSITORY  = var.dataform_repository
      DATAFORM_WORKSPACE   = var.dataform_workspace
    }
  }
}
