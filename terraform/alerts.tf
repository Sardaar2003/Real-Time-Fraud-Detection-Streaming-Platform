# ==========================================
# 1. Notification Channels
# ==========================================

# Email Notification Channel for Ops/Engineering
resource "google_monitoring_notification_channel" "ops_email" {
  display_name = "Operations Team Email Channel"
  type         = "email"
  
  labels = {
    email_address = "mahindar.singh.chennai@gmail.com"
  }
}

# ==========================================
# 2. Custom Log-Based Metrics
# ==========================================

# Track Schema Validation failures in pipeline logs
resource "google_logging_metric" "schema_validation_failures" {
  name        = "dataflow/schema_validation_failures"
  filter      = "resource.type=\"dataflow_step\" AND (textPayload:\"Schema validation failed\" OR textPayload:\"JSON decoding failed\")"
  description = "Counts the occurrences of invalid or corrupt messages routed to the DLQ"
}

# ==========================================
# 3. Alerting Policies
# ==========================================

# Alert Policy: Dataflow Streaming System Lag is too high (Pipeline Congestion)
resource "google_monitoring_alert_policy" "dataflow_lag_alert" {
  display_name = "Dataflow Pipeline System Lag Alert (${var.environment})"
  combiner     = "OR"
  
  conditions {
    display_name = "System Lag > 60s"
    
    condition_threshold {
      # Dataflow job system lag metric
      filter          = "metric.type=\"dataflow.googleapis.com/job/system_lag\" resource.type=\"dataflow_job\""
      duration        = "60s" # Must persist for 1 minute
      comparison      = "COMPARISON_GT"
      threshold_value = 60.0 # 60 seconds
      
      trigger {
        count = 1
      }
      
      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MAX"
        cross_series_reducer = "REDUCE_MAX"
        group_by_fields      = ["resource.label.job_name"]
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.ops_email.name]

  documentation {
    content   = "The Dataflow streaming pipeline system lag has exceeded 60 seconds. This indicates the pipeline is falling behind ingestion rates. Check worker CPU utilization and possible scaling limits."
    mime_type = "text/markdown"
  }
}

# Alert Policy: Pub/Sub Ingestion Backlog is piling up (Downstream Consumer Outage)
resource "google_monitoring_alert_policy" "pubsub_backlog_alert" {
  display_name = "Pub/Sub Subscription Backlog Alert (${var.environment})"
  combiner     = "OR"

  conditions {
    display_name = "Unacknowledged messages > 10,000"

    condition_threshold {
      filter          = "metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" resource.type=\"pubsub_subscription\" resource.label.subscription_id=\"transactions-sub-${var.environment}\""
      duration        = "120s" # Must persist for 2 minutes
      comparison      = "COMPARISON_GT"
      threshold_value = 10000.0 # 10k messages backlogged

      trigger {
        count = 1
      }

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.ops_email.name]

  documentation {
    content   = "The message backlog in Pub/Sub subscription `transactions-sub-${var.environment}` has exceeded 10,000 messages. This suggests the Dataflow pipeline is offline, crashed, or unable to acknowledge messages. Check Dataflow pipeline logs."
    mime_type = "text/markdown"
  }
}

# ==========================================
# 4. Custom Monitoring Dashboard
# ==========================================

resource "google_monitoring_dashboard" "streaming_platform_dashboard" {
  dashboard_json = <<EOF
{
  "displayName": "Real-Time Fraud Detection Platform Dashboard (${var.environment})",
  "gridLayout": {
    "columns": 2,
    "widgets": [
      {
        "title": "Pub/Sub Ingestion Throughput (Publish Rate)",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "metric.type=\"pubsub.googleapis.com/topic/send_message_operation_count\" resource.type=\"pubsub_topic\" resource.label.topic_id=\"transactions-topic-${var.environment}\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_RATE"
                  }
                }
              },
              "plotType": "LINE"
            }
          ]
        }
      },
      {
        "title": "Pub/Sub Subscription Backlog (Unacknowledged Messages)",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\" resource.type=\"pubsub_subscription\" resource.label.subscription_id=\"transactions-sub-${var.environment}\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_MEAN"
                  }
                }
              },
              "plotType": "LINE"
            }
          ]
        }
      },
      {
        "title": "Dataflow Pipeline System Lag (Seconds)",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "metric.type=\"dataflow.googleapis.com/job/system_lag\" resource.type=\"dataflow_job\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_MAX"
                  }
                }
              },
              "plotType": "LINE"
            }
          ]
        }
      },
      {
        "title": "Dataflow Schema Validation Failures (DLQ Counts)",
        "xyChart": {
          "dataSets": [
            {
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "metric.type=\"logging.googleapis.com/user/dataflow/schema_validation_failures\" resource.type=\"dataflow_step\"",
                  "aggregation": {
                    "alignmentPeriod": "60s",
                    "perSeriesAligner": "ALIGN_COUNT"
                  }
                }
              },
              "plotType": "LINE"
            }
          ]
        }
      }
    ]
  }
}
EOF
}
