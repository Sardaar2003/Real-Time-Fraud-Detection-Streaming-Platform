#!/usr/bin/env python3
"""
Unit Tests for Dataflow Streaming Pipeline
Uses Apache Beam's TestPipeline to verify custom DoFns and routing logic.
Now tests stateful feature engineering and Vertex AI inference scoring.
"""

import json
import os
import unittest
from unittest.mock import MagicMock, patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from pipeline import ParseAndValidateTxDoFn, StatefulFeatureDoFn, VertexAIInferenceDoFn, FormatForBigQueryFn, FormatForFraudAlertsFn


class DataflowPipelineTest(unittest.TestCase):
    
    def setUp(self):
        # Locate schema file relative to this test script
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'transaction_event.json')
        with open(schema_path, 'r') as f:
            self.schema_str = f.read()

        # Define basic mock transaction
        self.valid_event = {
            "transaction_id": "00000000-0000-0000-0000-000000000000",
            "timestamp": "2026-06-14T06:00:00Z",
            "card_id": "11111111-1111-1111-1111-111111111111",
            "amount": 42.50,
            "merchant_id": "m-starbucks-123",
            "merchant_category": "dining",
            "location": {
                "latitude": 40.7128,
                "longitude": -74.0060
            },
            "device_id": "dev-999999"
        }

        # Define transaction missing required fields
        self.invalid_event_missing_field = {
            "transaction_id": "00000000-0000-0000-0000-000000000000",
            "timestamp": "2026-06-14T06:00:00Z",
            "card_id": "11111111-1111-1111-1111-111111111111",
            "merchant_id": "m-starbucks-123",
            "merchant_category": "dining",
            "location": {
                "latitude": 40.7128,
                "longitude": -74.0060
            }
        }

        self.corrupt_bytes = b"{invalid_json_bytes"

    def test_parse_and_validate_success(self):
        """Verifies that a valid JSON event passes validation and yields in main output."""
        valid_bytes = json.dumps(self.valid_event).encode('utf-8')
        
        with TestPipeline() as p:
            input_pcoll = p | beam.Create([valid_bytes])
            results = input_pcoll | beam.ParDo(
                ParseAndValidateTxDoFn(self.schema_str)
            ).with_outputs(ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID, main='valid')
            
            assert_that(results.valid, equal_to([self.valid_event]))
            assert_that(results[ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID], equal_to([]))

    def test_parse_and_validate_invalid_schema(self):
        """Verifies that events with missing fields fail schema validation and route to DLQ."""
        invalid_bytes = json.dumps(self.invalid_event_missing_field).encode('utf-8')
        
        with TestPipeline() as p:
            input_pcoll = p | beam.Create([invalid_bytes])
            results = input_pcoll | beam.ParDo(
                ParseAndValidateTxDoFn(self.schema_str)
            ).with_outputs(ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID, main='valid')
            
            assert_that(results.valid, equal_to([]))
            assert_that(results[ParseAndValidateTxDoFn.OUTPUT_TAG_INVALID], equal_to([invalid_bytes]))

    def test_stateful_rolling_aggregations(self):
        """Tests rolling transaction counts and sums inside the 10-minute sliding window."""
        card_id = "card-aaa"
        
        tx1 = {**self.valid_event, "card_id": card_id, "timestamp": "2026-06-14T06:00:00Z", "amount": 10.00}
        tx2 = {**self.valid_event, "card_id": card_id, "timestamp": "2026-06-14T06:02:00Z", "amount": 20.00}
        tx3 = {**self.valid_event, "card_id": card_id, "timestamp": "2026-06-14T06:04:00Z", "amount": 15.00}
        
        with TestPipeline() as p:
            input_pcoll = p | beam.Create([
                (card_id, tx1),
                (card_id, tx2),
                (card_id, tx3)
            ])
            
            enriched = input_pcoll | beam.ParDo(StatefulFeatureDoFn())
            
            def check_features(elements):
                sorted_el = sorted(elements, key=lambda x: x['timestamp'])
                self.assertEqual(len(sorted_el), 3)
                
                self.assertEqual(sorted_el[0]['tx_count_10m'], 1)
                self.assertEqual(sorted_el[0]['tx_sum_10m'], 10.00)
                self.assertEqual(sorted_el[0]['impossible_travel'], 0)

                self.assertEqual(sorted_el[1]['tx_count_10m'], 2)
                self.assertEqual(sorted_el[1]['tx_sum_10m'], 30.00)
                self.assertEqual(sorted_el[1]['impossible_travel'], 0)

                self.assertEqual(sorted_el[2]['tx_count_10m'], 3)
                self.assertEqual(sorted_el[2]['tx_sum_10m'], 45.00)
                self.assertEqual(sorted_el[2]['impossible_travel'], 0)

            assert_that(enriched, check_features)

    def test_stateful_window_eviction(self):
        """Verifies that transactions older than 10 minutes (600s) are evicted from state aggregates."""
        card_id = "card-bbb"
        
        tx1 = {**self.valid_event, "card_id": card_id, "timestamp": "2026-06-14T06:00:00Z", "amount": 10.00}
        tx2 = {**self.valid_event, "card_id": card_id, "timestamp": "2026-06-14T06:11:00Z", "amount": 25.00}

        with TestPipeline() as p:
            input_pcoll = p | beam.Create([
                (card_id, tx1),
                (card_id, tx2)
            ])
            
            enriched = input_pcoll | beam.ParDo(StatefulFeatureDoFn())
            
            def check_eviction(elements):
                sorted_el = sorted(elements, key=lambda x: x['timestamp'])
                self.assertEqual(len(sorted_el), 2)
                
                self.assertEqual(sorted_el[0]['tx_count_10m'], 1)
                self.assertEqual(sorted_el[0]['tx_sum_10m'], 10.00)
                
                self.assertEqual(sorted_el[1]['tx_count_10m'], 1)
                self.assertEqual(sorted_el[1]['tx_sum_10m'], 25.00)

            assert_that(enriched, check_eviction)

    def test_impossible_travel_detection(self):
        """Verifies that impossible velocities trigger impossible_travel=1."""
        card_id = "card-ccc"
        
        tx1 = {
            **self.valid_event, 
            "card_id": card_id, 
            "timestamp": "2026-06-14T06:00:00Z",
            "location": {"latitude": 40.7128, "longitude": -74.0060}
        }
        tx2 = {
            **self.valid_event, 
            "card_id": card_id, 
            "timestamp": "2026-06-14T06:01:00Z",
            "location": {"latitude": 34.0522, "longitude": -118.2437}
        }

        with TestPipeline() as p:
            input_pcoll = p | beam.Create([
                (card_id, tx1),
                (card_id, tx2)
            ])
            
            enriched = input_pcoll | beam.ParDo(StatefulFeatureDoFn())
            
            def check_impossible_travel(elements):
                sorted_el = sorted(elements, key=lambda x: x['timestamp'])
                self.assertEqual(len(sorted_el), 2)
                
                self.assertEqual(sorted_el[0]['impossible_travel'], 0)
                self.assertEqual(sorted_el[1]['impossible_travel'], 1)

            assert_that(enriched, check_impossible_travel)

    @patch('google.cloud.aiplatform.Endpoint')
    def test_inference_success(self, mock_endpoint_class):
        """Verifies that successful Endpoint calls record predictions and flag high scores."""
        # Configure mocked endpoint client instances
        mock_endpoint_instance = MagicMock()
        mock_endpoint_class.return_value = mock_endpoint_instance
        
        # Configure mock prediction response (return 85% probability)
        mock_response = MagicMock()
        mock_response.predictions = [0.85]
        mock_endpoint_instance.predict.return_value = mock_response

        enriched_event = {
            **self.valid_event,
            "tx_count_10m": 1,
            "tx_sum_10m": 42.50,
            "impossible_travel": 0
        }

        with TestPipeline() as p:
            input_pcoll = p | beam.Create([enriched_event])
            
            # Use valid endpoint string to invoke client initialization
            scored = input_pcoll | beam.ParDo(
                VertexAIInferenceDoFn("projects/test/locations/us-central1/endpoints/123")
            )
            
            def check_scores(elements):
                self.assertEqual(len(elements), 1)
                el = elements[0]
                self.assertEqual(el['fraud_probability'], 0.85)
                self.assertEqual(el['is_fraud'], True)
                self.assertEqual(el['model_version'], "vertex-ai-endpoint")

            assert_that(scored, check_scores)

    def test_inference_fallback(self):
        """Verifies that offline endpoints/placeholders trigger local heuristic scoring fallback."""
        card_id = "card-fallback"
        
        # Normal event (should receive baseline 0.01)
        tx_normal = {
            **self.valid_event,
            "card_id": card_id,
            "amount": 25.00,
            "tx_count_10m": 1,
            "tx_sum_10m": 25.00,
            "impossible_travel": 0
        }
        
        # High value anomaly (should receive 0.85 and is_fraud=True)
        tx_high_val = {
            **self.valid_event,
            "card_id": card_id,
            "amount": 1500.00,
            "tx_count_10m": 1,
            "tx_sum_10m": 1500.00,
            "impossible_travel": 0
        }

        with TestPipeline() as p:
            input_pcoll = p | beam.Create([tx_normal, tx_high_val])
            
            # Pass placeholder endpoint ID to force local fallback
            scored = input_pcoll | beam.ParDo(VertexAIInferenceDoFn("endpoint-id-placeholder"))
            
            def check_fallbacks(elements):
                sorted_el = sorted(elements, key=lambda x: x['amount'])
                self.assertEqual(len(sorted_el), 2)
                
                # Normal transaction fallback metrics
                self.assertEqual(sorted_el[0]['amount'], 25.00)
                self.assertEqual(sorted_el[0]['fraud_probability'], 0.01)
                self.assertEqual(sorted_el[0]['is_fraud'], False)
                self.assertEqual(sorted_el[0]['model_version'], "local-heuristic-fallback")
                
                # High value anomaly fallback metrics
                self.assertEqual(sorted_el[1]['amount'], 1500.00)
                self.assertEqual(sorted_el[1]['fraud_probability'], 0.85)
                self.assertEqual(sorted_el[1]['is_fraud'], True)
                self.assertEqual(sorted_el[1]['model_version'], "local-heuristic-fallback")

            assert_that(scored, check_fallbacks)

    def test_format_for_bigquery(self):
        """Verifies that format step maps new feature columns and prediction outputs correctly."""
        enriched_event = {
            **self.valid_event,
            "tx_count_10m": 3,
            "tx_sum_10m": 72.50,
            "impossible_travel": 1,
            "fraud_probability": 0.95,
            "is_fraud": True,
            "model_version": "vertex-ai-endpoint"
        }
        
        with TestPipeline() as p:
            input_pcoll = p | beam.Create([enriched_event])
            formatted = input_pcoll | beam.ParDo(FormatForBigQueryFn())
            
            def check_bq_mapping(rows):
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual(row['tx_count_10m'], 3)
                self.assertEqual(row['tx_sum_10m'], 72.50)
                self.assertEqual(row['impossible_travel'], 1)
                self.assertEqual(row['fraud_probability'], 0.95)
                self.assertEqual(row['is_fraud'], True)
                self.assertEqual(row['model_version'], "vertex-ai-endpoint")
                self.assertNotIn('location', row)
                self.assertIn('processed_timestamp', row)

            assert_that(formatted, check_bq_mapping)

    def test_format_for_fraud_alerts(self):
        """Verifies that format step maps fraud alert fields correctly."""
        enriched_event = {
            **self.valid_event,
            "tx_count_10m": 3,
            "tx_sum_10m": 72.50,
            "impossible_travel": 1,
            "fraud_probability": 0.95,
            "is_fraud": True,
            "model_version": "vertex-ai-endpoint"
        }
        
        with TestPipeline() as p:
            input_pcoll = p | beam.Create([enriched_event])
            formatted = input_pcoll | beam.ParDo(FormatForFraudAlertsFn())
            
            def check_alerts_mapping(rows):
                self.assertEqual(len(rows), 1)
                row = rows[0]
                self.assertEqual(row['transaction_id'], self.valid_event['transaction_id'])
                self.assertEqual(row['timestamp'], self.valid_event['timestamp'])
                self.assertEqual(row['card_id'], self.valid_event['card_id'])
                self.assertEqual(row['amount'], self.valid_event['amount'])
                self.assertEqual(row['merchant_id'], self.valid_event['merchant_id'])
                self.assertEqual(row['fraud_probability'], 0.95)
                self.assertNotIn('location', row)
                self.assertIn('processed_timestamp', row)

            assert_that(formatted, check_alerts_mapping)


if __name__ == '__main__':
    unittest.main()
