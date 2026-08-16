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

data "google_project" "current" {
  project_id = var.project_id
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

resource "google_project_service" "dataform_api" {
  service            = "dataform.googleapis.com"
  disable_on_destroy = false
}

# Dataform strict act-as mode requires an explicit identity for every workflow
# invocation and requires both the caller and Dataform's service agent to be
# allowed to impersonate that identity.
resource "google_service_account_iam_member" "function_runtime_can_act_as_dataform" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.service_account_email}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${var.service_account_email}"
}

resource "google_service_account_iam_member" "dataform_service_agent_can_use_execution_account" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.service_account_email}"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"
}

resource "google_service_account_iam_member" "dataform_service_agent_can_create_execution_tokens" {
  service_account_id = "projects/${var.project_id}/serviceAccounts/${var.service_account_email}"
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"
}
