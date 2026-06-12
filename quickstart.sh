#!/bin/bash

# Quick Start Script for Real-Time Ride Analytics Pipeline
# This script validates and starts the entire stack

set -e

echo "🚗 Real-Time Ride Analytics Pipeline - Quick Start"
echo "=================================================="
echo ""

# Check Python
echo "📋 Checking prerequisites..."
python validate_setup.py

if [ $? -ne 0 ]; then
    echo ""
    echo "❌ Validation failed. Please fix issues above."
    exit 1
fi

echo ""
echo "✓ All checks passed!"
echo ""

# Create .env if not exists
if [ ! -f .env ]; then
    echo "📝 Creating .env from template..."
    cp .env.example .env
    echo "✓ .env created. Review and update if needed."
fi

echo ""
echo "🛠️ Building Docker images sequentially to avoid parallel BuildKit conflicts..."
docker compose build producer
docker compose build streamlit
echo ""
echo "🚀 Starting Docker Compose stack..."
echo ""

docker compose up -d

echo ""
echo "✓ Services starting..."
echo ""
echo "📊 Dashboard will be available at: http://localhost:8501"
echo "⏳ Wait 10-15 seconds for services to fully initialize..."
echo ""

# Wait for services
sleep 15

echo "🔍 Checking service health..."
docker compose ps

echo ""
echo "✓ Setup complete!"
echo ""
echo "📖 Next steps:"
echo "   1. Open http://localhost:8501 in your browser"
echo "   2. Monitor logs: docker compose logs -f"
echo "   3. Stop services: docker compose down"
echo ""
echo "ℹ️  For more commands, run: make help"
