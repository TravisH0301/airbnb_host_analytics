# Configure Terraform
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

# Configure GCP provider
provider "google" {
  credentials = file(var.gcp_key_path)
  project     = var.project
  region      = var.region
  zone        = var.zone
}

# Configure GCS
## Create GCS bucket
resource "google_storage_bucket" "ingress" {
  name                        = "airbnb_ingress"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = false
}
## Configure GCS bucket
resource "google_storage_bucket_acl" "ingress-acl" {
  bucket         = google_storage_bucket.ingress.name
  predefined_acl = "private"
}

# Configure Big Query
## Create Big Query datasets
resource "google_bigquery_dataset" "raw-data" {
  dataset_id  = "airbnb_raw_data"
  description = "Dataset for raw data"
  location    = var.region
}
resource "google_bigquery_dataset" "data-mart" {
  dataset_id  = "airbnb_data_mart"
  description = "Dataset for data mart"
  location    = var.region
}
resource "google_bigquery_dataset" "metric-layer" {
  dataset_id  = "airbnb_metric_layer"
  description = "Dataset for metric layer"
  location    = var.region
}