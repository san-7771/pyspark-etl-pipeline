# main.tf
# Defines the entire GCP infrastructure for your ETL project.
# Running `terraform apply` creates everything from scratch.
# Running `terraform destroy` deletes everything cleanly.

# ── Provider Configuration ─────────────────────────────────────
# Tells Terraform which cloud to talk to and how to authenticate
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  # Store Terraform state in GCS
  # State file tracks what Terraform has already created
  # Storing in GCS means teammates share the same state
  backend "gcs" {
    bucket = "de-pipeline-sanyam-2026"      # your existing bucket
    prefix = "terraform/state"              # folder inside bucket
  }
}

provider "google" {
  credentials = file("../gcp-key.json")    # your service account key
  project     = var.project_id
  region      = var.region
}

# ── Variables ──────────────────────────────────────────────────
# Like function parameters — change values without editing main.tf
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "de-learning-project-499519"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-south2"
}

variable "bucket_name" {
  description = "GCS bucket for ETL data"
  type        = string
  default     = "de-pipeline-sanyam-2026"
}

# ── GCS Bucket ─────────────────────────────────────────────────
# Your bucket already exists — use `terraform import` to bring it
# under Terraform management without recreating it
resource "google_storage_bucket" "etl_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = false    # prevents accidental deletion of data

  # Versioning — keeps old versions of files (extra safety net)
  versioning {
    enabled = true
  }

  # Lifecycle rule — delete files in /temp/ after 7 days
  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["temp/"]
    }
    action {
      type = "Delete"
    }
  }
}

# ── BigQuery Dataset ───────────────────────────────────────────
resource "google_bigquery_dataset" "orders_warehouse" {
  dataset_id    = "orders_warehouse"
  friendly_name = "Orders Data Warehouse"
  description   = "ETL pipeline output — Gold layer + dbt models"
  location      = var.region

  # Who can access this dataset
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
}

# ── Pub/Sub Topic ──────────────────────────────────────────────
resource "google_pubsub_topic" "orders_topic" {
  name = "orders-topic"

  # How long Pub/Sub retains undelivered messages
  message_retention_duration = "604800s"  # 7 days
}

resource "google_pubsub_subscription" "orders_subscription" {
  name  = "orders-subscription"
  topic = google_pubsub_topic.orders_topic.name

  # How long subscriber has to acknowledge before redelivery
  ack_deadline_seconds = 60

  # How long undelivered messages are retained
  message_retention_duration = "604800s"

  # Automatically delete subscription after 31 days of inactivity
  expiration_policy {
    ttl = "2678400s"
  }
}

# ── Outputs ────────────────────────────────────────────────────
# Values printed after terraform apply
# Useful for scripts that need these values
output "bucket_url" {
  value       = "gs://${google_storage_bucket.etl_bucket.name}"
  description = "GCS bucket URL for ETL data"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.orders_warehouse.dataset_id
  description = "BigQuery dataset ID"
}

output "pubsub_topic" {
  value       = google_pubsub_topic.orders_topic.id
  description = "Pub/Sub topic ID"
}