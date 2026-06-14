# Requirements Documentation: Real-Time Fraud Detection Streaming Platform

This document outlines the requirements for the Real-Time Fraud Detection Streaming Platform. It acts as the product and technical specification for the project.

---

## 1. Functional Requirements (FR)

Functional requirements define the specific behaviors and capabilities that the platform must deliver.

### Ingestion & Data Generation
* **Continuous Event Simulation**: The platform must support a transaction simulator capable of generating synthetic transaction events with realistic attributes (card number, merchant, amount, location, timestamp).
* **Ingestion Layer**: The ingestion layer must consume events via a secure API and ingest them into a messaging queue with sub-second ingestion latency.
* **Schema Validation**: Every transaction event entering the system must be validated against a strict, pre-defined schema. Malformed messages must be routed to a Dead Letter Queue (DLQ) for auditing.

### Stream Processing & Feature Engineering
* **Real-time Enrichment**: The streaming pipeline must parse transaction events and compute streaming features (e.g., transaction count or total spent by a customer in sliding time windows).
* **Stateful Aggregations**: The system must track sliding time windows of transactions per customer/card to compute feature values for inference.

### Model Inference
* **Real-time Prediction**: For every valid transaction event, the system must call a machine learning model to predict the probability of fraud.
* **Low Latency Prediction**: Real-time predictions must be generated and returned in time to block or flag the transaction.

### Storage & Downstream Routing
* **Raw Data Archival**: Every incoming transaction, along with its calculated features, prediction score, and model version, must be written to a historical database for auditability and model retraining.
* **Fraud Alerting & Routing**: Transactions predicted as fraud (probability above a configured threshold, e.g., 0.8) must be routed to a dedicated table/topic for immediate alert triggering.

### Monitoring & Alerting
* **System Dashboard**: A visual dashboard must display throughput, CPU utilization, database ingestion rate, and system latency.
* **Alert Notifications**: Operations teams must be notified within 2 minutes if the volume of fraud events exceeds a threshold or if the streaming pipeline lags.

---

## 2. Non-Functional Requirements (NFR)

Non-functional requirements specify the performance, quality, and operational constraints of the system.

### Latency
* **End-to-End Processing Latency**: The total time from when a transaction is published to Pub/Sub to when the prediction is completed and recorded must be **less than 500ms** (target: < 200ms) under nominal load.
* **Inference Endpoint Latency**: The P99 prediction latency of the Vertex AI endpoint must be **less than 50ms**.

### Throughput & Scalability
* **Nominal Throughput**: The system must process a baseline of 100 transactions per second (TPS).
* **Peak Throughput**: The system must auto-scale to handle a temporary burst of up to 1,000 TPS without dropping messages or exceeding latency targets.

### High Availability & Durability
* **Uptime**: The system must achieve 99.9% availability for ingestion and processing.
* **Zero Data Loss**: In the event of downstream system outages, the ingestion layer must buffer events for up to 7 days, guaranteeing no transaction data is lost.

---

## 3. Security Requirements

### Identity and Access Management (IAM)
* **Principle of Least Privilege**: Each component must use a dedicated Google Cloud Service Account with the absolute minimum roles required (e.g., the Dataflow runner service account should only have Pub/Sub subscriber, BigQuery writer, and Vertex AI predictor permissions).
* **No Hardcoded Credentials**: No passwords, API keys, or GCP service account key files (`JSON`) may be hardcoded or checked into source control. All authorization must use Google Application Default Credentials (ADC) or Secret Manager.

### Encryption
* **Encryption in Transit**: All data moving between components (e.g., client to Pub/Sub, Dataflow to Vertex AI) must be encrypted using Transport Layer Security (TLS 1.2+).
* **Encryption at Rest**: All data stored in Pub/Sub, BigQuery, and Cloud Storage must be encrypted at rest using Google-managed encryption keys (or Customer-Managed Encryption Keys - CMEK, where required by compliance).

---

## 4. Scalability & Cost Considerations

* **Serverless Architecture**: Every component chosen (Pub/Sub, Dataflow, Vertex AI, BigQuery) is serverless, meaning costs scale linearly with usage.
* **Auto-Scaling**: Dataflow auto-scaling ensures we do not pay for idle compute when transaction volumes drop (e.g., overnight).
* **Storage Tiering**: BigQuery automatically optimizes cost by moving tables that have not been modified for 90 days to long-term storage, reducing storage costs by 50%.
