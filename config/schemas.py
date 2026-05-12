"""
Avro schemas for ride events and metrics.
Using Avro ensures schema evolution and compatibility across producers/consumers.
"""

RIDE_EVENT_SCHEMA = {
    "type": "record",
    "name": "RideEvent",
    "namespace": "ride_analytics",
    "fields": [
        {"name": "event_id", "type": "string", "doc": "Unique event identifier"},
        {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": ["REQUEST", "PICKUP", "DROPOFF", "CANCELLED"]}},
        {"name": "driver_id", "type": "string"},
        {"name": "passenger_id", "type": "string"},
        {"name": "ride_id", "type": "string"},
        {"name": "timestamp", "type": "long", "doc": "Unix timestamp in milliseconds"},
        {"name": "pickup_latitude", "type": "double"},
        {"name": "pickup_longitude", "type": "double"},
        {"name": "dropoff_latitude", "type": ["null", "double"], "default": None},
        {"name": "dropoff_longitude", "type": ["null", "double"], "default": None},
        {"name": "distance_miles", "type": ["null", "double"], "default": None},
        {"name": "estimated_fare", "type": ["null", "double"], "default": None},
        {"name": "zone_id", "type": "string", "doc": "Geofence zone identifier"},
        {"name": "surge_multiplier", "type": ["null", "double"], "default": 1.0},
    ]
}

RIDE_METRICS_SCHEMA = {
    "type": "record",
    "name": "RideMetrics",
    "namespace": "ride_analytics",
    "fields": [
        {"name": "window_start", "type": "long", "doc": "5-min window start (Unix ms)"},
        {"name": "window_end", "type": "long", "doc": "5-min window end (Unix ms)"},
        {"name": "zone_id", "type": "string"},
        {"name": "request_count", "type": "int"},
        {"name": "pickup_count", "type": "int"},
        {"name": "completion_rate", "type": "double"},
        {"name": "avg_wait_time_sec", "type": "double"},
        {"name": "surge_triggered", "type": "boolean"},
        {"name": "surge_multiplier", "type": "double"},
        {"name": "avg_distance_miles", "type": "double"},
        {"name": "metrics_timestamp", "type": "long", "doc": "When metrics were calculated"},
    ]
}

# Kafka topic configuration
TOPICS_CONFIG = {
    "ride_events": {
        "partitions": 10,
        "replication_factor": 1,
        "retention_ms": 86400000,  # 24 hours
    },
    "ride_metrics": {
        "partitions": 3,
        "replication_factor": 1,
        "retention_ms": 604800000,  # 7 days
    },
    "surge_alerts": {
        "partitions": 5,
        "replication_factor": 1,
        "retention_ms": 3600000,  # 1 hour
    }
}
