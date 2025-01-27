variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = ""
  sensitive = true

}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-central2"
}

variable "bucket_name" {
  description = "GCS Bucket Name"
  type        = string
  default     = ""

}

variable "api_football_key" {
  description = "API Football Key"
  type        = string
  sensitive   = true
  default     = ""

}

variable "discord_webhook_url" {
  description = "Discord Webhook URL"
  type        = string
  sensitive   = true
  default     = ""
}

variable "google_maps_api_key" {
  description = "Google Maps API Key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "reddit_client_id" {
  description = "Reddit Client ID"
  type        = string
  sensitive   = true
  default     = ""
}

variable "reddit_client_secret" {
  description = "Reddit Client Secret"
  type        = string
  sensitive   = true
  default     = ""
}

variable "dataform_repository" {
  description = "Dataform Repository Name"
  type        = string
  default     = ""
}

variable "dataform_workspace" {
  description = "Dataform Workspace Name"
  type        = string
  default     = ""
}

variable "service_account_email" {
  description = "Service Account Email for Pub/Sub push authentication"
  type        = string
  sensitive = true
  default     = ""
}

