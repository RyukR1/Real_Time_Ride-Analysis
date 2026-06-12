"""
Spark Structured Streaming: Process ride events with:
- Windowed aggregation (5-min windows)
- Geofencing (zone detection)
- Watermarking (handle late data up to 2 minutes)
- Surge detection (requests > threshold)
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    from_json, col, window, count, approx_count_distinct, 
    when, explode, struct, to_timestamp, current_timestamp,
    first, last, broadcast
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

from config.settings import settings



def create_spark_session() -> SparkSession:
    """Create and configure Spark session."""
    return (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.streaming.kafka.maxRatePerPartition", "10000")
        .getOrCreate()
    )


def define_schema() -> StructType:
    """Define the schema for ride events from Kafka."""
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("ride_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", LongType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("trip_distance_km", DoubleType(), True),
        StructField("surge_multiplier", DoubleType(), True),
    ])


def geofence_zone(lat_col, lon_col):
    """
    Simple geofencing: Map coordinates to zones using native Spark SQL functions.
    """
    zones = {
        "manhattan": ((40.7000, 40.8300), (-74.0300, -73.9200)),
        "brooklyn": ((40.5700, 40.7000), (-74.0200, -73.8800)),
        "queens": ((40.6500, 40.8200), (-73.9200, -73.7000)),
        "bronx": ((40.7900, 40.9500), (-73.9700, -73.7800)),
        "staten_island": ((40.5000, 40.6500), (-74.3000, -74.0500)),
    }
    
    expr = None
    for zone_name, ((lat_min, lat_max), (lon_min, lon_max)) in zones.items():
        cond = (lat_col >= lat_min) & (lat_col <= lat_max) & (lon_col >= lon_min) & (lon_col <= lon_max)
        if expr is None:
            expr = when(cond, zone_name)
        else:
            expr = expr.when(cond, zone_name)
            
    return expr.otherwise("unknown")


def write_to_redis(batch_df, batch_id):
    """
    Collects the streaming microbatch results to the driver
    and writes them directly to Redis.
    """
    rows = batch_df.collect()
    if not rows:
        return
        
    import redis
    import json
    from datetime import datetime, timezone
    
    try:
        client = redis.Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            decode_responses=True,
            socket_connect_timeout=5
        )
        for row in rows:
            zone_id = row["zone_id"]
            metrics = {
                "ride_request_count": int(row["ride_request_count"]),
                "active_drivers": int(row["active_drivers"]),
                "surge_flag": bool(row["surge_flag"]),
                "suggested_multiplier": float(row["suggested_multiplier"]),
                "window_start": str(row["window_start"]),
                "window_end": str(row["window_end"]),
                "processed_at": str(row["processed_at"])
            }
            key = f"surge:{zone_id}:current"
            client.setex(
                key,
                300,  # 5 minutes TTL
                json.dumps({
                    **metrics,
                    "updated_at": datetime.now(timezone.utc).isoformat()
                })
            )
        print(f"✓ Spark successfully wrote batch {batch_id} with {len(rows)} rows to Redis")
    except Exception as e:
        print(f"❌ Spark error writing batch {batch_id} to Redis: {e}")


def process_ride_stream(spark: SparkSession) -> None:
    """
    Main streaming pipeline:
    1. Read from Kafka
    2. Parse events
    3. Apply watermark for late data
    4. Aggregate by zone and time window
    5. Detect surge conditions
    6. Write results
    """
    
    schema = define_schema()
    
    # Step 1: Read from Kafka
    print("📖 Reading from Kafka...")
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    
    # Step 2: Parse JSON events
    print("🔍 Parsing events...")
    df_events = (
        df_raw
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_timestamp", (col("timestamp") / 1000).cast("timestamp"))
        .withColumn("zone", when(
            (col("latitude").isNotNull()) & (col("longitude").isNotNull()),
            geofence_zone(col("latitude"), col("longitude"))
        ).otherwise("unknown"))
    )
    
    # Step 3: Apply watermark for late data (handle delays up to 2 minutes)
    print("⏰ Applying watermark...")
    df_watermarked = (
        df_events
        .withWatermark("event_timestamp", f"{settings.watermark_delay_seconds} seconds")
    )
    
    # Step 4: Windowed aggregation (5-minute windows)
    print("🪟 Creating windowed aggregations...")
    window_duration = f"{settings.window_duration_minutes} minutes"
    
    df_windowed = (
        df_watermarked
        .groupBy(
            window(col("event_timestamp"), window_duration, window_duration),
            col("zone")
        )
        .agg(
            count(when(col("event_type") == "RIDE_REQUESTED", 1)).alias("ride_request_count"),
            approx_count_distinct("driver_id").alias("active_drivers"),
            first("surge_multiplier", ignorenulls=True).alias("avg_surge_multiplier")
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )
    
    # Step 5: Detect surge conditions
    print("📈 Detecting surge conditions...")
    df_surge = (
        df_windowed
        .withColumn("surge_flag", col("ride_request_count") > settings.surge_threshold)
        .withColumn(
            "suggested_multiplier",
            when(
                col("surge_flag"),
                1.0 + ((col("ride_request_count") - settings.surge_threshold) / settings.surge_threshold * 0.5)
            ).otherwise(1.0)
        )
        .select(
            col("zone").alias("zone_id"),
            col("window_start"),
            col("window_end"),
            col("ride_request_count"),
            col("active_drivers"),
            col("surge_flag"),
            col("suggested_multiplier"),
            current_timestamp().alias("processed_at")
        )
    )
    
    # Step 6: Write to multiple sinks
    print("💾 Setting up data sinks...")
    
    # Sink 1: Console (for debugging)
    query_console = (
        df_surge
        .writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 20)
        .start()
    )
    
    # Sink 2: Parquet on local filesystem (simulating Iceberg)
    # In production, use: .format("iceberg").mode("append")
    query_parquet = (
        df_surge
        .writeStream
        .format("parquet")
        .option("path", "/tmp/ride_analytics_surge_metrics")
        .option("checkpointLocation", "/tmp/ride_analytics_checkpoint_surge")
        .start()
    )
    
    # Sink 3: CSV for analysis
    query_csv = (
        df_surge
        .writeStream
        .format("csv")
        .option("path", "/tmp/ride_analytics_metrics_csv")
        .option("header", "true")
        .option("checkpointLocation", "/tmp/ride_analytics_checkpoint_csv")
        .start()
    )
    
    # Sink 4: Redis for Streamlit
    query_redis = (
        df_surge
        .writeStream
        .outputMode("update")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", "/tmp/ride_analytics_checkpoint_redis")
        .start()
    )
    
    print("\n✓ Streaming queries started.")
    print("   Monitoring surge pricing in real-time...\n")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        process_ride_stream(spark)
    except KeyboardInterrupt:
        print("\n⚠️  Streaming job interrupted.")
    finally:
        spark.stop()
        print("✓ Spark session stopped.")
