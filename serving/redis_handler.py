"""
Redis Handler: Store hot metrics for sub-millisecond lookups.
Caches current surge rates, active driver counts, and zone metrics.
"""

import json
import redis
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings


class RedisMetricsStore:
    """Manages Redis operations for ride analytics metrics."""
    
    def __init__(self):
        """Initialize Redis connection."""
        try:
            self.client = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
            # Test connection
            self.client.ping()
            print(f"✓ Connected to Redis at {settings.redis_host}:{settings.redis_port}")
        except redis.ConnectionError as e:
            print(f"⚠️  Could not connect to Redis: {e}")
            print("   Running in offline mode.")
            self.client = None
    
    def set_surge_metrics(self, zone_id: str, metrics: Dict[str, Any], ttl_seconds: int = 300) -> bool:
        """
        Store surge metrics for a zone.
        
        Args:
            zone_id: Geographic zone identifier
            metrics: Metrics dict with surge info
            ttl_seconds: Time-to-live (5 minutes default)
        
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            key = f"surge:{zone_id}:current"
            self.client.setex(
                key,
                ttl_seconds,
                json.dumps({
                    **metrics,
                    "updated_at": datetime.utcnow().isoformat()
                })
            )
            return True
        except Exception as e:
            print(f"❌ Error storing surge metrics: {e}")
            return False
    
    def get_surge_metrics(self, zone_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve current surge metrics for a zone.
        
        Args:
            zone_id: Geographic zone identifier
        
        Returns:
            Metrics dict or None if not found
        """
        if not self.client:
            return None
        
        try:
            key = f"surge:{zone_id}:current"
            data = self.client.get(key)
            return json.loads(data) if data else None
        except Exception as e:
            print(f"❌ Error retrieving surge metrics: {e}")
            return None
    
    def get_all_surge_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Retrieve surge metrics for all zones."""
        if not self.client:
            return {}
        
        try:
            pattern = "surge:*:current"
            keys = self.client.keys(pattern)
            metrics = {}
            
            for key in keys:
                zone_id = key.split(":")[1]
                data = self.client.get(key)
                if data:
                    metrics[zone_id] = json.loads(data)
            
            return metrics
        except Exception as e:
            print(f"❌ Error retrieving all metrics: {e}")
            return {}
    
    def set_driver_location(self, driver_id: str, latitude: float, longitude: float, ttl_seconds: int = 60) -> bool:
        """
        Store current driver location for geospatial queries.
        
        Args:
            driver_id: Unique driver identifier
            latitude: Current latitude
            longitude: Current longitude
            ttl_seconds: Time-to-live (1 minute default)
        
        Returns:
            True if successful
        """
        if not self.client:
            return False
        
        try:
            key = f"driver:{driver_id}:location"
            self.client.setex(
                key,
                ttl_seconds,
                json.dumps({
                    "latitude": latitude,
                    "longitude": longitude,
                    "timestamp": datetime.utcnow().isoformat()
                })
            )
            return True
        except Exception as e:
            print(f"❌ Error storing driver location: {e}")
            return False
    
    def get_active_drivers_in_zone(self, zone_id: str) -> int:
        """
        Count active drivers in a zone (from Redis cache).
        
        Args:
            zone_id: Geographic zone identifier
        
        Returns:
            Number of active drivers
        """
        if not self.client:
            return 0
        
        try:
            # In production, use Redis geospatial commands
            # For now, query surge metrics cache
            metrics = self.get_surge_metrics(zone_id)
            return metrics.get("active_drivers", 0) if metrics else 0
        except Exception as e:
            print(f"❌ Error counting active drivers: {e}")
            return 0
    
    def increment_surge_counter(self, zone_id: str) -> int:
        """
        Increment surge event counter for analytics.
        
        Args:
            zone_id: Geographic zone identifier
        
        Returns:
            Updated counter value
        """
        if not self.client:
            return 0
        
        try:
            key = f"surge:{zone_id}:counter"
            count = self.client.incr(key)
            self.client.expire(key, 3600)  # Expire after 1 hour
            return count
        except Exception as e:
            print(f"❌ Error incrementing counter: {e}")
            return 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get Redis cache statistics."""
        if not self.client:
            return {"status": "offline"}
        
        try:
            info = self.client.info()
            keys = self.client.dbsize()
            
            return {
                "status": "online",
                "used_memory_mb": info.get("used_memory", 0) / (1024 * 1024),
                "total_keys": keys,
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception as e:
            print(f"❌ Error getting cache stats: {e}")
            return {"status": "error"}
    
    def clear_all_metrics(self) -> bool:
        """Clear all metrics from Redis (use with caution)."""
        if not self.client:
            return False
        
        try:
            # Only clear our namespaced keys
            pattern = "surge:*"
            keys = self.client.keys(pattern)
            if keys:
                self.client.delete(*keys)
                print(f"✓ Cleared {len(keys)} surge metric keys from Redis")
            return True
        except Exception as e:
            print(f"❌ Error clearing metrics: {e}")
            return False


# Singleton instance
redis_store = RedisMetricsStore()
