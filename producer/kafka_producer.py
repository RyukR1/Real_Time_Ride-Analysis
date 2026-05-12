"""
Kafka Producer: Generates synthetic ride events.

Simulates 100+ events/second with realistic ride request patterns:
- Random driver locations (NYC area)
- Random pickup/dropoff zones
- Temporal patterns (surge during peak hours)
"""

import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from random import random, randint, choice, gauss
from typing import Dict, Any

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
from geopy.distance import geodesic

from config.settings import settings

# Initialize Faker for realistic data
fake = Faker()

# NYC neighborhoods (lat, lon, zone_name)
ZONES = {
    "manhattan": {"lat": 40.7580, "lon": -73.9855, "zone_id": "zone_manhattan"},
    "brooklyn": {"lat": 40.6782, "lon": -73.9442, "zone_id": "zone_brooklyn"},
    "queens": {"lat": 40.7282, "lon": -73.7949, "zone_id": "zone_queens"},
    "bronx": {"lat": 40.8448, "lon": -73.8648, "zone_id": "zone_bronx"},
    "staten_island": {"lat": 40.5795, "lon": -74.1502, "zone_id": "zone_staten_island"},
}

EVENT_TYPES = ["RIDE_REQUESTED", "DRIVER_PICKUP", "RIDE_COMPLETED"]


def random_location_near_zone(zone_name: str, radius_km: float = 2.0) -> tuple:
    """Generate a random location near a zone using Gaussian distribution."""
    zone = ZONES[zone_name]
    
    # Convert km to degrees (rough approximation)
    lat_offset = (gauss(0, radius_km / 111) if random() > 0.5 else 0)
    lon_offset = (gauss(0, radius_km / (111 * abs(__import__('math').cos(__import__('math').radians(zone['lat']))))) 
                  if random() > 0.5 else 0)
    
    return (zone["lat"] + lat_offset, zone["lon"] + lon_offset)


def calculate_trip_distance(pickup: tuple, dropoff: tuple) -> float:
    """Calculate distance between two coordinates in km."""
    return geodesic(pickup, dropoff).kilometers


def generate_ride_event(driver_id: str, event_num: int) -> Dict[str, Any]:
    """Generate a synthetic ride event."""
    event_type = choice(EVENT_TYPES)
    zone_name = choice(list(ZONES.keys()))
    current_location = random_location_near_zone(zone_name)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "driver_id": driver_id,
        "ride_id": f"ride_{driver_id}_{event_num}",
        "latitude": current_location[0],
        "longitude": current_location[1],
        "timestamp": int(datetime.utcnow().timestamp() * 1000),
    }
    
    # Add location details for different event types
    if event_type == "RIDE_REQUESTED":
        pickup_loc = random_location_near_zone(zone_name, radius_km=1.0)
        dropoff_loc = random_location_near_zone(choice(list(ZONES.keys())), radius_km=1.0)
        event["pickup_latitude"] = pickup_loc[0]
        event["pickup_longitude"] = pickup_loc[1]
        event["dropoff_latitude"] = dropoff_loc[0]
        event["dropoff_longitude"] = dropoff_loc[1]
        event["trip_distance_km"] = calculate_trip_distance(pickup_loc, dropoff_loc)
        
        # Simulate surge pricing (higher during peak hours)
        hour = datetime.utcnow().hour
        if 7 <= hour <= 9 or 17 <= hour <= 19:  # Rush hours
            event["surge_multiplier"] = round(1.0 + random() * 0.5, 2)
        else:
            event["surge_multiplier"] = 1.0
    
    return event


def create_producer(max_retries: int = 5) -> KafkaProducer:
    """
    Create and configure Kafka producer with exponential backoff retry logic.
    
    Args:
        max_retries: Maximum number of connection attempts
        
    Returns:
        Configured KafkaProducer instance
        
    Raises:
        NoBrokersAvailable: If unable to connect after all retries
    """
    # Determine compression type (snappy preferred, fallback to gzip)
    compression_type = "gzip"
    try:
        import snappy
        compression_type = "snappy"
        print("✓ Snappy compression available")
    except (ImportError, ModuleNotFoundError):
        print("⚠️  Snappy not available, using gzip compression")
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔗 Connecting to Kafka (attempt {attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                compression_type=compression_type,
                request_timeout_ms=10000,  # 10 second timeout
                connections_max_idle_ms=540000,  # 9 minutes
            )
            print(f"✓ Successfully connected to Kafka broker")
            return producer
        except NoBrokersAvailable as e:
            if attempt == max_retries:
                print(f"✗ Failed to connect to Kafka after {max_retries} attempts")
                raise
            
            # Exponential backoff: 2s, 4s, 8s, 16s, 32s
            wait_time = 2 ** attempt
            print(f"⚠️  No brokers available. Retrying in {wait_time}s...")
            time.sleep(wait_time)


def produce_events(duration_seconds: int = 60, events_per_sec: int = 100):
    """
    Continuously produce ride events to Kafka.
    
    Args:
        duration_seconds: How long to run (0 = infinite)
        events_per_sec: Events to generate per second
    """
    producer = create_producer()
    
    # Generate driver pool
    drivers = [f"driver_{i}" for i in range(settings.num_drivers)]
    
    start_time = time.time()
    event_count = 0
    event_num = 0
    
    print(f"🚗 Starting Kafka Producer...")
    print(f"   Topic: {settings.kafka_topic}")
    print(f"   Kafka Servers: {settings.kafka_bootstrap_servers}")
    print(f"   Drivers: {len(drivers)}")
    print(f"   Rate: {events_per_sec} events/sec\n")
    
    try:
        while True:
            # Check duration limit
            if duration_seconds > 0 and (time.time() - start_time) > duration_seconds:
                print(f"\n⏱️  Duration limit reached. Produced {event_count} events.")
                break
            
            # Produce batch of events
            batch_size = max(1, events_per_sec // 10)  # Send in 10 batches per second
            
            for _ in range(batch_size):
                driver = choice(drivers)
                event = generate_ride_event(driver, event_num)
                
                producer.send(settings.kafka_topic, value=event)
                event_count += 1
                event_num += 1
            
            # Print stats every 100 events
            if event_count % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = event_count / elapsed
                print(f"✓ Events produced: {event_count} | Rate: {actual_rate:.1f} events/sec")
            
            # Throttle to maintain target rate
            time.sleep(1.0 / 10)  # 10 batches per second
    
    except KeyboardInterrupt:
        print(f"\n⚠️  Producer interrupted. Total events: {event_count}")
    finally:
        producer.close()
        print("✓ Producer closed.")


if __name__ == "__main__":
    produce_events(duration_seconds=0)  # Run indefinitely
