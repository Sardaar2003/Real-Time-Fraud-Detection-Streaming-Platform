provider "google" {
  project = var.project_id
  region  = var.region
}

# ==========================================
# 1. Pub/Sub Resources (Ingestion Layer)
# ==========================================

# Raw Transactions Ingestion Topic
resource "google_pubsub_topic" "transactions" {
  name = "transactions-topic-${var.environment}"

  labels = {
    environment = var.environment
    project     = "fraud-detection"
  }
}

# Raw Transactions Subscription for Dataflow
resource "google_pubsub_subscription" "transactions_sub" {
  name  = "transactions-sub-${var.environment}"
  topic = google_pubsub_topic.transactions.name

  # Retain acknowledged messages for 7 days (durability requirement)
  message_retention_duration = "604800s"
  retain_acked_messages      = false

  # Acknowledgment deadline
  ack_deadline_seconds = 60

  expiration_policy {
    # Never expire subscription due to inactivity
    ttl = ""
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Dead Letter Queue (DLQ) Topic for Malformed Messages
resource "google_pubsub_topic" "dlq" {
  name = "transactions-dlq-${var.environment}"

  labels = {
    environment = var.environment
    project     = "fraud-detection"
  }
}

# DLQ Subscription to audit failed messages
resource "google_pubsub_subscription" "dlq_sub" {
  name  = "transactions-dlq-sub-${var.environment}"
  topic = google_pubsub_topic.dlq.name

  message_retention_duration = "604800s"
  ack_deadline_seconds       = 60

  expiration_policy {
    ttl = ""
  }
}

# ==========================================
# 2. Cloud Storage (Staging & Temp Dataflow)
# ==========================================

resource "google_storage_bucket" "dataflow_bucket" {
  name          = "dataflow-staging-${var.project_id}-${var.environment}"
  location      = var.region
  force_destroy = true # Convenient for dev environments

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# ==========================================
# 3. BigQuery Resources (Storage Layer)
# ==========================================

resource "google_bigquery_dataset" "fraud_dataset" {
  dataset_id                  = "fraud_detection_${var.environment}"
  friendly_name               = "Fraud Detection Dataset"
  description                 = "Dataset containing real-time credit card transactions and model evaluation outputs"
  location                    = var.region
  default_table_expiration_ms = null # Retain data indefinitely
}

resource "google_bigquery_table" "raw_transactions" {
  dataset_id = google_bigquery_dataset.fraud_dataset.dataset_id
  table_id   = "raw_transactions"
  
  # Partition by transaction timestamp for optimized analytical querying
  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  schema = <<EOF
[
  {
    "name": "transaction_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique transaction UUID"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "ISO 8601 transaction execution time"
  },
  {
    "name": "card_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Customer credit card identifier"
  },
  {
    "name": "amount",
    "type": "FLOAT",
    "mode": "REQUIRED",
    "description": "Transaction dollar amount"
  },
  {
    "name": "merchant_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique merchant identifier"
  },
  {
    "name": "merchant_category",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Type of merchant"
  },
  {
    "name": "location_latitude",
    "type": "FLOAT",
    "mode": "REQUIRED",
    "description": "Latitude coordinate of transaction"
  },
  {
    "name": "location_longitude",
    "type": "FLOAT",
    "mode": "REQUIRED",
    "description": "Longitude coordinate of transaction"
  },
  {
    "name": "device_id",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Terminal device identifier"
  },
  {
    "name": "fraud_probability",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "Probability score assigned by ML model"
  },
  {
    "name": "is_fraud",
    "type": "BOOLEAN",
    "mode": "NULLABLE",
    "description": "Flag representing whether fraud_probability exceeds decision threshold"
  },
  {
    "name": "model_version",
    "type": "STRING",
    "mode": "NULLABLE",
    "description": "Version string of prediction model hosted on Vertex AI"
  },
  {
    "name": "tx_count_10m",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Rolling count of transactions for the card in the last 10 minutes"
  },
  {
    "name": "tx_sum_10m",
    "type": "FLOAT",
    "mode": "NULLABLE",
    "description": "Rolling sum of transaction amounts for the card in the last 10 minutes"
  },
  {
    "name": "impossible_travel",
    "type": "INTEGER",
    "mode": "NULLABLE",
    "description": "Binary flag indicating if impossible travel velocity was detected (0 or 1)"
  },
  {
    "name": "processed_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Timestamp when transaction was processed by Dataflow"
  }
]
EOF
}

resource "google_bigquery_table" "fraud_alerts" {
  dataset_id = google_bigquery_dataset.fraud_dataset.dataset_id
  table_id   = "fraud_alerts"

  time_partitioning {
    type  = "DAY"
    field = "timestamp"
  }

  schema = <<EOF
[
  {
    "name": "transaction_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique transaction UUID"
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "ISO 8601 transaction execution time"
  },
  {
    "name": "card_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Customer credit card identifier"
  },
  {
    "name": "amount",
    "type": "FLOAT",
    "mode": "REQUIRED",
    "description": "Transaction dollar amount"
  },
  {
    "name": "merchant_id",
    "type": "STRING",
    "mode": "REQUIRED",
    "description": "Unique merchant identifier"
  },
  {
    "name": "fraud_probability",
    "type": "FLOAT",
    "mode": "REQUIRED",
    "description": "Fraud score from Vertex AI Endpoint"
  },
  {
    "name": "processed_timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED",
    "description": "Time processed by Dataflow"
  }
]
EOF
}
