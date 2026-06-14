variable "project_id" {
  type        = string
  description = "The GCP Project ID where resources will be created"
  default     = "fraud-prediction-499405"
}

variable "region" {
  type        = string
  description = "The GCP region to deploy resources in"
  default     = "us-central1"
}

variable "environment" {
  type        = string
  description = "Deployment environment (e.g. dev, staging, prod)"
  default     = "dev"
}
