variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-central2"
}

variable "bucket_name" {
  description = "GCS Bucket Name"
  type        = string
}

variable "api_football_key" {
  description = "API Football Key"
  type        = string
  sensitive   = true
}

variable "discord_webhook_url" {
  description = "Discord Webhook URL"
  type        = string
  sensitive   = true
}

variable "google_maps_api_key" {
  description = "Google Maps API Key"
  type        = string
  sensitive   = true
}

variable "reddit_client_id" {
  description = "Reddit Client ID"
  type        = string
  sensitive   = true
}

variable "reddit_client_secret" {
  description = "Reddit Client Secret"
  type        = string
  sensitive   = true
}

variable "dataform_repository" {
  description = "Dataform Repository Name"
  type        = string
}

variable "dataform_workspace" {
  description = "Dataform Workspace Name"
  type        = string
}

variable "service_account_email" {
  description = "Service Account Email for Pub/Sub push authentication"
  type        = string
}

