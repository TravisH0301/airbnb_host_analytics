# Create GCP provider variables
variable "gcp_key_path" {
  type        = string
  description = "Placeholder variable for for key path"
}
variable "project" {
  type        = string
  description = "Project name"
  default     = "airbnb-host-analytics"
}
variable "region" {
  type        = string
  description = "Project region"
  default     = "australia-southeast1"
}
variable "zone" {
  type        = string
  description = "Project zone"
  default     = "australia-southeast1-b"
}
