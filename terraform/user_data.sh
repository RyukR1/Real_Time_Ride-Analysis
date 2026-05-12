#!/bin/bash
# User data script for Spark processor EC2 instance

set -e

echo "Installing dependencies..."
apt-get update
apt-get install -y \
    python3-pip \
    python3-dev \
    build-essential \
    openjdk-11-jdk \
    curl \
    git

echo "Installing Python packages..."
pip3 install --upgrade pip
pip3 install pyspark==3.5.0 kafka-python==2.0.2 pandas==2.0.3

echo "Creating app directory..."
mkdir -p /opt/ride-analytics
cd /opt/ride-analytics

echo "Cloning repository..."
# git clone https://github.com/your-username/Real_Time_Ride-Analysis.git .

echo "Setup complete!"
echo "Kafka bootstrap servers: ${kafka_bootstrap}}"
