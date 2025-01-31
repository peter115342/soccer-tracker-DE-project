terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "cloudfunctions_api" {
  service = "cloudfunctions.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "cloudbuild_api" {
  service = "cloudbuild.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "artifactregistry_api" {
  service = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

data "archive_file" "trigger_dataplex_scans" {
  type        = "zip"
  source_dir  = "../cloud_functions/dataplex/trigger_dataplex_scans_function"
  output_path = "../cloud_functions/dataplex/trigger_dataplex_scans_function.zip"
}
