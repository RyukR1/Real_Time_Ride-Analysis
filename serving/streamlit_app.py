"""
Streamlit Dashboard: Live visualization of ride analytics.
- Real-time surge pricing by zone
- Active drivers count
- Demand patterns
- Historical trends
"""

import streamlit as st
import pandas as pd
import numpy as np
import sys
from pathlib import Path
from datetime import datetime, timedelta
import time
from typing import Dict, List

# Add app directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis
from redis_handler import redis_store
from config.settings import settings

# Page configuration
st.set_page_config(
    page_title="🚗 Real-Time Ride Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .surge-on {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    .surge-off {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=5)
def load_metrics_data() -> pd.DataFrame:
    """Load current metrics from Redis."""
    metrics = redis_store.get_all_surge_metrics()
    
    if not metrics:
        return pd.DataFrame(columns=[
            "Zone", "Ride Requests", "Active Drivers", 
            "Surge Active", "Multiplier", "Updated At"
        ])
    
    data = []
    for zone_id, metric in metrics.items():
        data.append({
            "Zone": zone_id.replace("_", " ").title(),
            "Ride Requests": metric.get("ride_request_count", 0),
            "Active Drivers": metric.get("active_drivers", 0),
            "Surge Active": metric.get("surge_flag", False),
            "Multiplier": metric.get("suggested_multiplier", 1.0),
            "Updated At": metric.get("updated_at", "N/A"),
        })
    
    return pd.DataFrame(data)


def display_header():
    """Display dashboard header."""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col1:
        st.image("https://img.icons8.com/color/96/000000/taxi.png", width=80)
    
    with col2:
        st.title("🚗 Real-Time Ride Analytics Pipeline")
        st.markdown("**Live monitoring of surge pricing, demand, and driver distribution**")
    
    with col3:
        redis_stats = redis_store.get_cache_stats()
        st.metric(
            "Cache Status",
            redis_stats.get("status", "unknown").upper(),
            f"{redis_stats.get('total_keys', 0)} keys"
        )


def display_metrics_grid():
    """Display key metrics in a grid."""
    df = load_metrics_data()
    
    if df.empty:
        st.warning("⏳ No metrics available yet. Waiting for data from Kafka...")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_requests = df["Ride Requests"].sum()
        st.metric("📞 Total Requests (5m)", f"{total_requests:,}")
    
    with col2:
        active_drivers = df["Active Drivers"].sum()
        st.metric("🚙 Active Drivers", f"{active_drivers:,}")
    
    with col3:
        surge_zones = len(df[df["Surge Active"]])
        st.metric("⚡ Surge Active Zones", f"{surge_zones}/{len(df)}")
    
    with col4:
        avg_multiplier = df["Multiplier"].mean()
        st.metric("💰 Avg Surge Multiplier", f"{avg_multiplier:.2f}x")


def display_zones_table():
    """Display detailed metrics by zone."""
    st.subheader("📍 Metrics by Zone (Updated Every 5 Seconds)")
    
    df = load_metrics_data()
    
    if df.empty:
        st.info("No data yet. Ensure Kafka producer and Spark processor are running.")
        return
    
    # Format the dataframe for display
    df_display = df.copy()
    df_display["Surge Active"] = df_display["Surge Active"].apply(
        lambda x: "🔴 YES" if x else "🟢 NO"
    )
    df_display["Multiplier"] = df_display["Multiplier"].apply(lambda x: f"{x:.2f}x")
    df_display["Ride Requests"] = df_display["Ride Requests"].apply(lambda x: f"{x:,}")
    df_display["Active Drivers"] = df_display["Active Drivers"].apply(lambda x: f"{x:,}")
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True
    )


def display_charts():
    """Display visualization charts."""
    st.subheader("📈 Demand & Surge Patterns")
    
    df = load_metrics_data()
    
    if df.empty:
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Ride requests by zone
        request_data = df.sort_values("Ride Requests", ascending=False)
        st.bar_chart(request_data.set_index("Zone")["Ride Requests"])
        st.caption("Ride Requests by Zone (5m window)")
    
    with col2:
        # Active drivers by zone
        driver_data = df.sort_values("Active Drivers", ascending=False)
        st.bar_chart(driver_data.set_index("Zone")["Active Drivers"])
        st.caption("Active Drivers by Zone")


def display_alerts():
    """Display surge alerts."""
    st.subheader("⚠️ Surge Alerts")
    
    df = load_metrics_data()
    surge_df = df[df["Surge Active"]]
    
    if surge_df.empty:
        st.success("✓ No surge pricing active at this moment")
    else:
        for _, row in surge_df.iterrows():
            st.warning(
                f"🔴 **{row['Zone']}**: Surge {row['Multiplier']:.2f}x | "
                f"{row['Ride Requests']:,} requests | "
                f"{row['Active Drivers']} drivers",
                icon="⚠️"
            )


def display_system_info():
    """Display system and configuration info."""
    with st.expander("ℹ️ System Information"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Configuration")
            st.json({
                "Kafka Bootstrap": settings.kafka_bootstrap_servers,
                "Topic": settings.kafka_topic,
                "Window Duration": f"{settings.window_duration_minutes}m",
                "Surge Threshold": settings.surge_threshold,
                "Watermark Delay": f"{settings.watermark_delay_seconds}s",
            })
        
        with col2:
            st.subheader("Redis Cache")
            stats = redis_store.get_cache_stats()
            st.json(stats)


def main():
    """Main Streamlit app."""
    display_header()
    
    st.markdown("---")
    
    display_metrics_grid()
    
    st.markdown("---")
    
    display_zones_table()
    
    st.markdown("---")
    
    display_charts()
    
    st.markdown("---")
    
    display_alerts()
    
    st.markdown("---")
    
    display_system_info()
    
    # Auto-refresh
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔄 Auto-Refresh")
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 1, 60, 5)
    
    st.sidebar.info(
        "💡 **Tip**: This dashboard refreshes in real-time from Redis cache. "
        "Ensure Kafka producer and Spark processor are running."
    )
    
    # Refresh loop
    placeholder = st.empty()
    time_placeholder = st.empty()
    
    while True:
        with placeholder.container():
            col1, col2, col3 = st.columns([2, 1, 2])
            
            with col2:
                time_placeholder.metric(
                    "Last Updated",
                    datetime.now().strftime("%H:%M:%S")
                )
        
        time.sleep(refresh_interval)


if __name__ == "__main__":
    main()
