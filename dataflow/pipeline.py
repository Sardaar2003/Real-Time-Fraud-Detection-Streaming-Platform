#!/usr/bin/env python3
"""
Dataflow Streaming Pipeline - Real-Time Fraud Detection Streaming Platform

Ingests credit card transaction streams from Google Cloud Pub/Sub,
validates events against a JSON schema, computes real-time stateful features,
performs real-time inference using a model deployed on a Vertex AI Endpoint,
routes failures to a Dead Letter Queue (DLQ), and streams enriched records
into Google Cloud BigQuery.
"""

import argparse
import datetime
import json
import logging
import math
import os
from typing import Dict, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.userstate import BagStateSpec, ReadModifyWriteStateSpec


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Computes the great-circle distance between two points on the Earth's surface
    using the Haversine formula. Returns distance in kilometers.
    """
    import math
    # Earth radius in kilometers
    R = 6371.0
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = (math.sin(dlat / 2.0) ** 2 + 
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * 
         math.sin(dlon / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    
    return R * c


def parse_iso_timestamp(ts_str: str) -> float:
    """
    Safely parses an ISO 8601 timestamp string into epoch seconds.
    Ensures compatibility across python versions by replacing 'Z' with '+00:00'.
    """
    from datetime import datetime
    cleaned = ts_str.replace('Z', '+00:00')
    return datetime.fromisoformat(cleaned).timestamp()


class ParseAndValidateTxDoFn(beam.DoFn):
    """
    Parses raw Pub/Sub message bytes, validates them against the JSON schema,
    and splits the stream into valid transactions (main output) and malformed
    transactions (tagged invalid output).
    """
    OUTPUT_TAG_INVALID = 'invalid_transactions'

    def __init__(self, schema_json_str: str):
        self.schema_json_str = schema_json_str

    def setup(self):
        """Initializes the validator once per worker thread for performance optimization."""
        import jsonschema
        import json
        schema = json.loads(self.schema_json_str)
        self.validator = jsonschema.Draft7Validator(schema)

    def process(self, element: bytes):
        """Processes a single raw transaction event."""
        import json
        import jsonschema

        try:
            # Decode message bytes
            decoded = element.decode('utf-8')
        except UnicodeDecodeError:
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, element)
            return

        try:
            event = json.loads(decoded)
            errors = list(self.validator.iter_errors(event))
            if errors:
                error_msg = "; ".join([err.message for err in errors])
                logging.warning(f"Schema validation failed: {error_msg}")
                yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, element)
            else:
                yield event
        except json.JSONDecodeError:
            logging.warning("JSON decoding failed. Routing to DLQ.")
            yield beam.pvalue.TaggedOutput(self.OUTPUT_TAG_INVALID, element)


class StatefulFeatureDoFn(beam.DoFn):
    """
    Computes real-time features on a per-card basis using stateful transforms:
    - tx_count_10m: Count of transactions in the last 10 minutes
    - tx_sum_10m: Sum of transaction amounts in the last 10 minutes
    - impossible_travel: Velocity check against previous transaction location
    """
    # BagState to store rolling transactions as tuples of (timestamp_seconds, amount)
    TX_BAG = BagStateSpec(
        'tx_bag', 
        beam.coders.TupleCoder((beam.coders.FloatCoder(), beam.coders.FloatCoder()))
    )
    
    # ValueState to track last known transaction coordinates: (latitude, longitude, timestamp_seconds)
    LAST_TX = ReadModifyWriteStateSpec(
        'last_tx', 
        beam.coders.TupleCoder((beam.coders.FloatCoder(), beam.coders.FloatCoder(), beam.coders.FloatCoder()))
    )

    def process(
        self, 
        element: Tuple[str, Dict], 
        tx_bag=beam.DoFn.StateParam(TX_BAG),
        last_tx=beam.DoFn.StateParam(LAST_TX)
    ):
        card_id, event = element
        
        # Extract event attributes
        curr_time = parse_iso_timestamp(event['timestamp'])
        curr_amount = float(event['amount'])
        curr_lat = float(event['location']['latitude'])
        curr_lon = float(event['location']['longitude'])

        # ---------------------------------------------
        # Feature 1 & 2: Rolling 10-Minute Count and Sum
        # ---------------------------------------------
        window_size_seconds = 600.0  # 10 minutes
        prev_transactions = list(tx_bag.read())
        
        active_transactions = []
        tx_count_10m = 0
        tx_sum_10m = 0.0

        for tx_time, tx_amt in prev_transactions:
            if curr_time - tx_time <= window_size_seconds:
                active_transactions.append((tx_time, tx_amt))
                tx_count_10m += 1
                tx_sum_10m += tx_amt
        
        # Include current transaction in rolling calculations
        tx_count_10m += 1
        tx_sum_10m += curr_amount
        active_transactions.append((curr_time, curr_amount))

        # Clear state bag and save updated window events
        tx_bag.clear()
        for item in active_transactions:
            tx_bag.add(item)

        # ---------------------------------------------
        # Feature 3: Location Velocity Check
        # ---------------------------------------------
        impossible_travel = 0
        prev_state = last_tx.read()

        if prev_state:
            prev_lat, prev_lon, prev_time = prev_state
            time_diff = curr_time - prev_time
            time_diff_abs = abs(time_diff)

            # Compute Haversine physical distance between coordinates
            dist_km = haversine_distance(prev_lat, prev_lon, curr_lat, curr_lon)

            if time_diff_abs < 1.0:
                # Sub-second transactions must occur within 500 meters
                if dist_km > 0.5:
                    impossible_travel = 1
            else:
                # Exceeds commercial flight speed threshold (800 km/h)
                velocity_kmh = dist_km / (time_diff_abs / 3600.0)
                if velocity_kmh > 800.0:
                    impossible_travel = 1

        # Save current location/time for the next iteration state check
        last_tx.write((curr_lat, curr_lon, curr_time))

        # Enrich transaction event dictionary with engineered features
        event['tx_count_10m'] = tx_count_10m
        event['tx_sum_10m'] = round(tx_sum_10m, 2)
        event['impossible_travel'] = impossible_travel

        yield event


class VertexAIInferenceDoFn(beam.DoFn):
    """
    Submits engineered features to Vertex AI online endpoint for fraud classification.
    Includes a robust local fallback logic for high availability.
    """
    def __init__(self, endpoint_id: str):
        self.endpoint_id = endpoint_id

    def setup(self):
        """Initializes Vertex AI Endpoint connection once per worker thread."""
        from google.cloud import aiplatform
        self.endpoint_client = None
        
        is_placeholder = ('placeholder' in self.endpoint_id.lower())
        
        if not is_placeholder:
            try:
                # endpoint_id should look like: projects/PROJECT/locations/LOCATION/endpoints/ENDPOINT
                self.endpoint_client = aiplatform.Endpoint(endpoint_name=self.endpoint_id)
            except Exception as e:
                logging.warning(f"Vertex AI Endpoint client setup failed: {e}. Using fallback heuristic.")

    def process(self, element: Dict):
        # Extract features matching the model training signature
        # Features: [amount, tx_count_10m, tx_sum_10m, impossible_travel]
        features = [
            float(element['amount']),
            int(element['tx_count_10m']),
            float(element['tx_sum_10m']),
            int(element['impossible_travel'])
        ]

        if self.endpoint_client:
            try:
                # Send inference call (synchronous online prediction)
                # Payload: {"instances": [[amount, tx_count_10m, tx_sum_10m, impossible_travel]]}
                response = self.endpoint_client.predict(instances=[features])
                
                preds = response.predictions
                if preds and isinstance(preds, list):
                    pred_val = preds[0]
                    # XGBoost prediction response parsing
                    if isinstance(pred_val, list):
                        # Multi-class output: [prob_non_fraud, prob_fraud]
                        fraud_prob = float(pred_val[1]) if len(pred_val) > 1 else float(pred_val[0])
                    else:
                        # Single probability class
                        fraud_prob = float(pred_val)
                else:
                    raise ValueError("Empty or invalid predictions received from Endpoint.")

                element['fraud_probability'] = fraud_prob
                element['is_fraud'] = bool(fraud_prob >= 0.80)
                element['model_version'] = "vertex-ai-endpoint"
                
            except Exception as e:
                logging.warning(f"Vertex AI online prediction call failed: {e}. executing local fallback.")
                self._run_fallback(element, features)
        else:
            self._run_fallback(element, features)

        yield element

    def _run_fallback(self, element: Dict, features: List):
        """
        Local fallback rule-based system mimicking the trained XGBoost model logic.
        Ensures high availability if connection to cloud endpoint fails.
        """
        amount = features[0]
        tx_count = features[1]
        tx_sum = features[2]
        impossible_travel = features[3]

        fraud_prob = 0.01  # Normal transaction baseline probability

        # Profile 1: High Amount Anomaly
        if amount > 1000.0:
            fraud_prob = max(fraud_prob, 0.85)

        # Profile 2: Impossible Location Velocity
        if impossible_travel == 1:
            fraud_prob = max(fraud_prob, 0.95)

        # Profile 3: In-Window Transaction Storm (Burst Anomaly)
        if tx_count >= 6 and tx_sum > 500.0:
            fraud_prob = max(fraud_prob, 0.92)
            
        # Profile 4: Medium Spend Storm
        if tx_count >= 3 and amount > 300.0:
            fraud_prob = max(fraud_prob, 0.82)

        element['fraud_probability'] = fraud_prob
        element['is_fraud'] = bool(fraud_prob >= 0.80)
        element['model_version'] = "local-heuristic-fallback"


class FormatForBigQueryFn(beam.DoFn):
    """
    Formats the enriched transaction dictionary to match the BigQuery schema.
    Flattens coordinates and maps ML scoring variables.
    """
    def process(self, element: Dict):
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        lat = element['location']['latitude']
        lon = element['location']['longitude']
        
        # Safe extraction of boolean is_fraud
        is_fraud_val = element.get("is_fraud")
        if is_fraud_val is not None:
            is_fraud_val = bool(is_fraud_val)

        formatted_row = {
            "transaction_id": element["transaction_id"],
            "timestamp": element["timestamp"],
            "card_id": element["card_id"],
            "amount": float(element["amount"]),
            "merchant_id": element["merchant_id"],
            "merchant_category": element["merchant_category"],
            "location_latitude": float(lat),
            "location_longitude": float(lon),
            "device_id": element.get("device_id"),
            # Inference parameters
            "fraud_probability": element.get("fraud_probability"),
            "is_fraud": is_fraud_val,
            "model_version": element.get("model_version"),
            # Real-time features
            "tx_count_10m": int(element["tx_count_10m"]),
            "tx_sum_10m": float(element["tx_sum_10m"]),
            "impossible_travel": int(element["impossible_travel"]),
            "processed_timestamp": now_iso
        }
        yield formatted_row


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_subscription',
        required=True,
        help='GCP Pub/Sub Subscription ID to consume transactions from'
    )
    parser.add_argument(
        '--output_table',
        required=True,
        help='BigQuery table destination (format: project:dataset.table)'
    )
    parser.add_argument(
        '--dlq_topic',
        required=True,
        help='GCP Pub/Sub Topic ID to route malformed events to'
    )
    parser.add_argument(
        '--schema_path',
        required=True,
        help='Local or GCS path to the transaction event JSON schema file'
    )
    parser.add_argument(
        '--endpoint_id',
        required=True,
        help='Vertex AI Endpoint resource name or endpoint ID'
    )

    known_args, pipeline_args = parser.parse_known_args()

    if not os.path.exists(known_args.schema_path):
        raise FileNotFoundError(f"Local schema file not found at: {known_args.schema_path}")
    
    with open(known_args.schema_path, "r") as f:
        schema_json_str = f.read()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    pipeline_options.view_as(SetupOptions).save_main_session = True
    setup_file_path = os.path.join(os.path.dirname(__file__), 'setup.py')
    pipeline_options.view_as(SetupOptions).setup_file = setup_file_path

    logging.info("Starting Dataflow Streaming Pipeline construction...")

    with beam.Pipeline(options=pipeline_options) as p:
        
        # 1. Read byte stream from Pub/Sub
        raw_events = (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        )

        # 2. Parse and Validate events, routing side outputs
        validation_results = (
            raw_events
            | "ParseAndValidate" >> beam.ParDo(
                ParseAndValidateTxDoFn(schema_json_str)
            ).with_outputs(ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID, main='valid_transactions')
        )

        valid_tx = validation_results.valid_transactions
        invalid_tx = validation_results[ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID]

        # 3. Key by card_id to prepare for stateful transforms
        keyed_transactions = (
            valid_tx
            | "KeyByCardId" >> beam.Map(lambda ev: (ev['card_id'], ev))
        )

        # 4. Compute real-time rolling state features
        enriched_transactions = (
            keyed_transactions
            | "StatefulFeatureEngineering" >> beam.ParDo(StatefulFeatureDoFn())
        )

        # 5. Call Vertex AI Endpoint online prediction service
        scored_transactions = (
            enriched_transactions
            | "VertexAIInference" >> beam.ParDo(VertexAIInferenceDoFn(known_args.endpoint_id))
        )

        # 6. Format and write enriched events to BigQuery
        _ = (
            scored_transactions
            | "FormatForBigQuery" >> beam.ParDo(FormatForBigQueryFn())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

        # 7. Route invalid events to DLQ
        _ = (
            invalid_tx
            | "WriteToDLQ" >> beam.io.WriteToPubSub(topic=known_args.dlq_topic)
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
