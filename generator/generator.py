#!/usr/bin/env python3
"""
Transaction Event Generator - Real-Time Fraud Detection Streaming Platform

Generates credit card transaction events, validates them against a JSON schema,
and publishes them to a Google Cloud Pub/Sub topic.
Simulates both normal transactions and various fraud patterns:
1. High Amount Spike (large value transaction)
2. Impossible Travel (rapid geographical location change)
3. Rapid Burst (multiple transactions within seconds on the same card)
"""

import argparse
import datetime
import json
import logging
import math
import os
import random
import time
import uuid
from typing import Dict, List, Tuple

from google.cloud import pubsub_v1
import jsonschema

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Sample static data for simulation
MERCHANT_CATEGORIES = [
    "retail", "grocery", "entertainment", "travel",
    "dining", "gas_station", "online_shopping", "other"
]

MERCHANTS = {
    "retail": ["Target", "Walmart", "Home Depot", "Best Buy"],
    "grocery": ["Kroger", "Whole Foods", "Safeway", "Trader Joes"],
    "entertainment": ["Netflix", "Spotify", "Ticketmaster", "AMC Theatres"],
    "travel": ["Delta Airlines", "Marriott", "Uber", "Hertz"],
    "dining": ["Starbucks", "McDonalds", "Olive Garden", "Chipotle"],
    "gas_station": ["Shell", "Chevron", "ExxonMobil", "BP"],
    "online_shopping": ["Amazon", "eBay", "Etsy", "Wayfair"],
    "other": ["Local Cleaners", "City Water", "Electric Utility"]
}

# Major US City Coordinates (lat, lon) for realistic location simulation
US_CITIES = [
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Chicago", 41.8781, -87.6298),
    ("Houston", 29.7604, -95.3698),
    ("Phoenix", 33.4484, -112.0740),
    ("Philadelphia", 39.9526, -75.1652),
    ("San Antonio", 29.4241, -98.4936),
    ("San Diego", 32.7157, -117.1611),
    ("Dallas", 32.7767, -96.7970),
    ("San Jose", 37.3382, -121.8863),
    ("Miami", 25.7617, -80.1918),
    ("Seattle", 47.6062, -122.3321),
    ("Denver", 39.7392, -104.9903)
]

def load_schema(schema_path: str) -> Dict:
    """Loads and returns the JSON Schema for validation."""
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")
    with open(schema_path, "r") as f:
        return json.load(f)

class CardRegistry:
    """Tracks state for generated card IDs to simulate sequential transactions."""
    def __init__(self, size: int = 500):
        # Pre-generate card IDs (UUIDs)
        self.card_ids = [str(uuid.uuid4()) for _ in range(size)]
        # Track last transaction location & timestamp for each card
        self.last_transactions: Dict[str, Tuple[float, float, float]] = {}

    def get_card(self) -> str:
        return random.choice(self.card_ids)

    def update_card(self, card_id: str, lat: float, lon: float, timestamp: float):
        self.last_transactions[card_id] = (lat, lon, timestamp)

    def get_last_state(self, card_id: str) -> Tuple[float, float, float] or None:
        return self.last_transactions.get(card_id)


def generate_transaction(registry: CardRegistry, fraud_rate: float) -> Tuple[Dict, str]:
    """
    Generates a single transaction event.
    With probability `fraud_rate`, generates a fraudulent pattern.
    """
    card_id = registry.get_card()
    now = time.time()
    now_iso = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).isoformat()
    
    # Check if card has a previous state
    last_state = registry.get_last_state(card_id)
    
    is_fraud = random.random() < fraud_rate
    fraud_type = "none"

    if is_fraud:
        # Determine fraud pattern
        fraud_type = random.choice(["high_amount", "impossible_travel", "rapid_burst"])

    # Base coordinates defaults
    city_name, base_lat, base_lon = random.choice(US_CITIES)
    lat = base_lat + random.uniform(-0.05, 0.05)
    lon = base_lon + random.uniform(-0.05, 0.05)
    
    category = random.choice(MERCHANT_CATEGORIES)
    merchant = random.choice(MERCHANTS[category])
    merchant_id = f"m-{merchant.lower().replace(' ', '-')}-{random.randint(100, 999)}"
    
    amount = round(random.expovariate(1.0 / 35.0) + 1.0, 2)
    # Guarantee standard transaction isn't ridiculously large
    if amount > 500.0:
        amount = round(random.uniform(5.0, 150.0), 2)

    # 1. Apply Fraud Patterns
    if is_fraud:
        if fraud_type == "high_amount":
            # Direct value anomaly
            amount = round(random.uniform(1200.0, 5000.0), 2)
            category = "travel" if random.random() < 0.5 else "online_shopping"
            merchant = random.choice(MERCHANTS[category])
            merchant_id = f"m-{merchant.lower().replace(' ', '-')}-{random.randint(1000, 9999)}"

        elif fraud_type == "impossible_travel" and last_state:
            # Shift location drastically from the last known state
            last_lat, last_lon, last_t = last_state
            # Pick a city that is far away
            far_city = random.choice([c for c in US_CITIES if c[0] != city_name])
            lat = far_city[1] + random.uniform(-0.02, 0.02)
            lon = far_city[2] + random.uniform(-0.02, 0.02)
            # Ensure timestamp is very close to last (e.g. 5 to 60 seconds later)
            time_diff = random.uniform(5.0, 60.0)
            now = last_t + time_diff
            now_iso = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).isoformat()
            amount = round(random.uniform(50.0, 300.0), 2)
            category = "dining" if random.random() < 0.5 else "retail"
            merchant = random.choice(MERCHANTS[category])

        elif fraud_type == "rapid_burst" and last_state:
            # Multiple events in rapid succession
            last_lat, last_lon, last_t = last_state
            lat = last_lat + random.uniform(-0.001, 0.001)
            lon = last_lon + random.uniform(-0.001, 0.001)
            time_diff = random.uniform(0.1, 2.0) # sub-2 seconds
            now = last_t + time_diff
            now_iso = datetime.datetime.fromtimestamp(now, tz=datetime.timezone.utc).isoformat()
            amount = round(random.uniform(80.0, 400.0), 2)
            category = "online_shopping"
            merchant = random.choice(MERCHANTS[category])

    # Update Registry
    registry.update_card(card_id, lat, lon, now)

    event = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": now_iso,
        "card_id": card_id,
        "amount": float(amount),
        "merchant_id": merchant_id,
        "merchant_category": category,
        "location": {
            "latitude": float(lat),
            "longitude": float(lon)
        },
        "device_id": f"dev-{random.randint(100000, 999999)}"
    }

    return event, fraud_type


def pubsub_callback(future: pubsub_v1.publisher.futures.Future):
    """Callback function for asynchronous publishing."""
    try:
        message_id = future.result()
        # Suppressed verbose logging to keep performance high
    except Exception as e:
        logger.error(f"Failed to publish event due to error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Transaction Event Generator")
    parser.add_argument("--project-id", type=str, default="fraud-prediction-499405", help="GCP Project ID")
    parser.add_argument("--topic-id", type=str, default="transactions-topic-dev", help="Pub/Sub Topic ID")
    parser.add_argument("--rate", type=float, default=2.0, help="Target transactions per second")
    parser.add_argument("--fraud-rate", type=float, default=0.05, help="Proportion of simulated transactions that are fraud")
    parser.add_argument("--schema-path", type=str, default="../schemas/transaction_event.json", help="Path to JSON Schema file")
    parser.add_argument("--max-events", type=int, default=0, help="Max events to generate (0 for infinite)")
    parser.add_argument("--dry-run", action="store_true", help="Print messages to stdout without publishing to Pub/Sub")

    args = parser.parse_args()

    logger.info("Starting Transaction Generator")
    logger.info(f"Targeting: Project: {args.project_id}, Topic: {args.topic_id}")
    logger.info(f"Configuration: Rate: {args.rate} events/sec, Fraud Rate: {args.fraud_rate * 100}%")

    # Load and validate schema path
    try:
        schema = load_schema(args.schema_path)
        validator = jsonschema.Draft7Validator(schema)
        logger.info(f"Successfully loaded and compiled JSON schema from {args.schema_path}")
    except Exception as e:
        logger.critical(f"Failed to initialize schema validator: {e}")
        return

    # Initialize Pub/Sub Publisher
    publisher = None
    topic_path = None
    if not args.dry_run:
        try:
            # Batch settings for high-throughput, low latency production setup
            batch_settings = pubsub_v1.types.BatchSettings(
                max_messages=100,
                max_bytes=1024 * 1024, # 1MB
                max_latency=0.05 # 50ms latency max
            )
            publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
            topic_path = publisher.topic_path(args.project_id, args.topic_id)
            logger.info(f"Initialized Pub/Sub publisher for topic: {topic_path}")
        except Exception as e:
            logger.critical(f"Failed to initialize Pub/Sub client: {e}")
            logger.info("Falling back to dry-run mode (printing only)")
            args.dry_run = True

    registry = CardRegistry()
    events_count = 0
    start_time = time.time()

    # Dynamic sleeper to maintain target rate
    sleep_time = 1.0 / args.rate

    try:
        while True:
            # Regular rate limiting
            loop_start = time.time()
            
            event, fraud_type = generate_transaction(registry, args.fraud_rate)
            
            # Validate against schema (strict schema boundary)
            try:
                validator.validate(event)
            except jsonschema.exceptions.ValidationError as err:
                logger.error(f"Event schema validation failed. Message discarded. Error: {err.message}")
                continue

            event_data = json.dumps(event).encode("utf-8")

            if args.dry_run:
                print(f"[DRY-RUN] [Fraud Type: {fraud_type}] {json.dumps(event, indent=2)}")
            else:
                # Add attributes to Pub/Sub message metadata (helpful for filtering/routing)
                future = publisher.publish(
                    topic_path, 
                    data=event_data, 
                    fraud_type=fraud_type, 
                    generator="python-simulator"
                )
                future.add_done_callback(pubsub_callback)

            events_count += 1
            if args.max_events > 0 and events_count >= args.max_events:
                logger.info(f"Reached limit of {args.max_events} events. Stopping.")
                break

            # Calculate actual sleep adjustment to maintain rate
            elapsed = time.time() - loop_start
            adjusted_sleep = max(0.0, sleep_time - elapsed)
            time.sleep(adjusted_sleep)

            # Performance reporting every 100 messages
            if events_count % 100 == 0:
                elapsed_total = time.time() - start_time
                avg_rate = events_count / elapsed_total
                logger.info(f"Generated {events_count} events total. Current average rate: {avg_rate:.2f} events/sec")

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Stopping generator.")
    finally:
        if publisher:
            logger.info("Flushing publisher connection...")
            # Flush publisher to ensure outstanding messages are sent
            # publisher.transport.close() in pubsub python SDK is handled natively on exit,
            # but we can sleep briefly to allow futures to resolve.
            time.sleep(2)
            logger.info("Done.")

if __name__ == "__main__":
    main()
