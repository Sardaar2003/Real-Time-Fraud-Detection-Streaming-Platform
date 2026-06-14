output "project_id" {
  value       = var.project_id
  description = "The GCP Project ID"
}

output "transactions_topic" {
  value       = google_pubsub_topic.transactions.name
  description = "The name of the Pub/Sub topic for raw transactions ingestion"
}

output "transactions_subscription" {
  value       = google_pubsub_subscription.transactions_sub.name
  description = "The name of the Pub/Sub subscription for Dataflow consumption"
}

output "dlq_topic" {
  value       = google_pubsub_topic.dlq.name
  description = "The name of the Pub/Sub topic for Dead Letter Queue"
}

output "dataflow_bucket" {
  value       = google_storage_bucket.dataflow_bucket.name
  description = "The GCS bucket for Dataflow temp and staging"
}

output "bigquery_dataset" {
  value       = google_bigquery_dataset.fraud_dataset.dataset_id
  description = "The BigQuery dataset ID"
}

output "raw_transactions_table" {
  value       = google_bigquery_table.raw_transactions.table_id
  description = "The BigQuery table name for raw transactions"
}

output "fraud_alerts_table" {
  value       = google_bigquery_table.fraud_alerts.table_id
  description = "The BigQuery table name for fraud alerts"
}
