"""
Configuration management for the ride analytics pipeline.
"""

import os
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Kafka Configuration
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "ride_events")
    
    # Database Configuration
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")
    db_name: str = os.getenv("DB_NAME", "ride_analytics")
    
    # Redis Configuration
    redis_host: str = os.getenv("REDIS_HOST", "localhost")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    
    # S3 / Iceberg Configuration
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "ride-analytics-bucket")
    s3_path: str = os.getenv("S3_PATH", "s3://ride-analytics-bucket/iceberg/")
    
    # Spark Configuration
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    spark_app_name: str = os.getenv("SPARK_APP_NAME", "RideAnalyticsPipeline")
    
    # Data Generation
    num_drivers: int = int(os.getenv("NUM_DRIVERS", "50"))
    events_per_second: int = int(os.getenv("EVENTS_PER_SECOND", "100"))
    city_center_lat: float = float(os.getenv("CITY_CENTER_LAT", "40.7128"))
    city_center_lon: float = float(os.getenv("CITY_CENTER_LON", "-74.0060"))
    city_radius_km: float = float(os.getenv("CITY_RADIUS_KM", "50"))
    
    # Processing Configuration
    window_duration_minutes: int = int(os.getenv("WINDOW_DURATION_MINUTES", "5"))
    surge_threshold: int = int(os.getenv("SURGE_THRESHOLD", "100"))
    watermark_delay_seconds: int = int(os.getenv("WATERMARK_DELAY_SECONDS", "120"))
    
    # Feature Flags
    enable_data_quality_checks: bool = os.getenv("ENABLE_DATA_QUALITY_CHECKS", "true").lower() == "true"
    debug_mode: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    
    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection string."""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @property
    def redis_url(self) -> str:
        """Construct Redis connection string."""
        return f"redis://{self.redis_host}:{self.redis_port}"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton settings instance
settings = Settings()
