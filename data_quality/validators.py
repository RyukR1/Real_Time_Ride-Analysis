"""
Data Quality Validators: Great Expectations rules for data validation.
Ensures data integrity across the pipeline.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime


class RideEventValidator:
    """Validates ride events for data quality."""
    
    # Constraints for validating events
    CONSTRAINTS = {
        "latitude": {"min": -90, "max": 90},
        "longitude": {"min": -180, "max": 180},
        "trip_distance_km": {"min": 0, "max": 5000},
        "surge_multiplier": {"min": 0.5, "max": 5.0},
    }
    
    def __init__(self, enable_checks: bool = True):
        """Initialize validator."""
        self.enable_checks = enable_checks
        self.violations = []
        self.checked_count = 0
    
    def validate_event(self, event: Dict[str, Any]) -> tuple[bool, Optional[List[str]]]:
        """
        Validate a single ride event.
        
        Args:
            event: Ride event dict
        
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        if not self.enable_checks:
            return True, None
        
        self.checked_count += 1
        errors = []
        
        # Required fields check
        required_fields = ["event_id", "event_type", "driver_id", "latitude", "longitude", "timestamp"]
        for field in required_fields:
            if field not in event or event[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Event type check
        valid_event_types = ["RIDE_REQUESTED", "DRIVER_PICKUP", "RIDE_COMPLETED"]
        if event.get("event_type") not in valid_event_types:
            errors.append(f"Invalid event_type: {event.get('event_type')}")
        
        # Coordinate validation
        lat = event.get("latitude")
        lon = event.get("longitude")
        
        if lat is not None:
            if not isinstance(lat, (int, float)) or not (-90 <= lat <= 90):
                errors.append(f"Invalid latitude: {lat}")
        
        if lon is not None:
            if not isinstance(lon, (int, float)) or not (-180 <= lon <= 180):
                errors.append(f"Invalid longitude: {lon}")
        
        # Trip distance check
        trip_dist = event.get("trip_distance_km")
        if trip_dist is not None:
            if not isinstance(trip_dist, (int, float)) or trip_dist < 0:
                errors.append(f"Invalid trip_distance_km: {trip_dist} (must be >= 0)")
        
        # Surge multiplier check
        surge = event.get("surge_multiplier")
        if surge is not None:
            if not isinstance(surge, (int, float)) or not (0.5 <= surge <= 5.0):
                errors.append(f"Invalid surge_multiplier: {surge} (must be 0.5-5.0)")
        
        # Timestamp validation
        timestamp = event.get("timestamp")
        if timestamp is not None:
            if not isinstance(timestamp, int) or timestamp <= 0:
                errors.append(f"Invalid timestamp: {timestamp}")
        
        # Optional location fields consistency
        if event.get("event_type") == "RIDE_REQUESTED":
            pickup_lat = event.get("pickup_latitude")
            pickup_lon = event.get("pickup_longitude")
            
            if (pickup_lat is None) != (pickup_lon is None):
                errors.append("pickup_latitude and pickup_longitude must both be present or both null")
        
        if errors:
            self.violations.append({
                "event_id": event.get("event_id"),
                "errors": errors,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        return len(errors) == 0, errors if errors else None
    
    def validate_batch(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate a batch of events.
        
        Args:
            events: List of ride events
        
        Returns:
            Summary stats
        """
        valid_count = 0
        invalid_count = 0
        
        for event in events:
            is_valid, _ = self.validate_event(event)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
        
        return {
            "total_checked": self.checked_count,
            "valid": valid_count,
            "invalid": invalid_count,
            "valid_percentage": (valid_count / len(events) * 100) if events else 0,
            "violations_count": len(self.violations),
        }
    
    def get_violations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent violations."""
        return self.violations[-limit:]
    
    def reset(self):
        """Reset validator state."""
        self.violations = []
        self.checked_count = 0


class AggregationValidator:
    """Validates aggregated metrics."""
    
    def validate_surge_metrics(self, metrics: Dict[str, Any]) -> tuple[bool, Optional[List[str]]]:
        """
        Validate surge metrics aggregation.
        
        Args:
            metrics: Aggregated metrics dict
        
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []
        
        # Required fields
        required = ["zone_id", "ride_request_count", "active_drivers", "surge_flag"]
        for field in required:
            if field not in metrics or metrics[field] is None:
                errors.append(f"Missing required field: {field}")
        
        # Type and range checks
        if not isinstance(metrics.get("ride_request_count"), int) or metrics.get("ride_request_count") < 0:
            errors.append("ride_request_count must be non-negative integer")
        
        if not isinstance(metrics.get("active_drivers"), int) or metrics.get("active_drivers") < 0:
            errors.append("active_drivers must be non-negative integer")
        
        if not isinstance(metrics.get("surge_flag"), bool):
            errors.append("surge_flag must be boolean")
        
        multiplier = metrics.get("suggested_multiplier")
        if multiplier is not None:
            if not isinstance(multiplier, (int, float)) or not (0.5 <= multiplier <= 5.0):
                errors.append("suggested_multiplier must be 0.5-5.0")
        
        return len(errors) == 0, errors if errors else None
    
    def validate_consistency(self, metrics: Dict[str, Any], threshold: int = 100) -> List[str]:
        """
        Validate internal consistency of metrics.
        
        Args:
            metrics: Aggregated metrics
            threshold: Surge threshold for consistency check
        
        Returns:
            List of consistency warnings
        """
        warnings = []
        
        # Surge flag should match threshold
        requests = metrics.get("ride_request_count", 0)
        surge_flag = metrics.get("surge_flag", False)
        
        if requests > threshold and not surge_flag:
            warnings.append(f"Inconsistency: {requests} requests > {threshold} but surge_flag=False")
        
        if requests <= threshold and surge_flag:
            warnings.append(f"Inconsistency: {requests} requests <= {threshold} but surge_flag=True")
        
        return warnings


# Singleton instances
event_validator = RideEventValidator()
aggregation_validator = AggregationValidator()


def get_validation_report() -> Dict[str, Any]:
    """Generate validation report."""
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "event_validation": {
            "total_checked": event_validator.checked_count,
            "violations": len(event_validator.violations),
        },
        "recent_violations": event_validator.get_violations(5),
    }
