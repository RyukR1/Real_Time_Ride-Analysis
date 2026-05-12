# Real-Time Ride Analytics Pipeline

A production-grade, streaming-first architecture for real-time ride analytics. Simulates 100+ events/sec with geospatial processing, surge detection, and live dashboards.

## 🏗️ Architecture

```
Producers (Python Faker)
        ↓
    Apache Kafka (event stream)
        ↓
PySpark Structured Streaming (windowed aggregatio/home/ryukr2/Real_Time_Ride-Analysisns, geofencing)
        ↓
    Dual Storage:/home/ryukr2/Real_Time_Ride-Analysis
    ├─→ Redis (hot metrics, <1ms lookups)
    ├─→ S3 + Iceberg (cold storage, historical analysis)
    └─→ PostgreSQL + PostGIS (geospatial queries)
        ↓
Streamlit Dashboard (live visualization)
```

## 🎯 Key Features

✅ **100+ Events/Second**: Simulates realistic NYC ride patterns  
✅ **Sub-Second Latency**: Redis caching for instant metric lookups  
✅ **Geospatial Processing**: Zone-based surge detection  
✅ **Windowed Aggregation**: 5-minute windows with watermarking  
✅ **Late Data Handling**: 2-minute grace period for delayed events  
✅ **Data Quality**: Great Expectations validators  
✅ **Infrastructure as Code**: Terraform for cloud deployment  
✅ **Production-Ready**: Docker Compose for local dev, Dockerfiles for production  

## 📊 Metrics Tracked

- **Surge Pricing**: Dynamic multiplier based on demand/supply
- **Request Volume**: 5-minute rolling counts by zone
- **Active Drivers**: Real-time driver distribution
- **Completion Rate**: Ride success metrics
- **ETA Accuracy**: Trip distance and duration

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- AWS CLI (for Terraform deployment)

### Local Development (Docker Compose)

```bash/home/ryukr2/Real_Time_Ride-Analysis
# Clone the repository
cd Real_Time_Ride-Analysis

# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Access dashboard
# Open http://localhost:8501 in your browser
```

### Manual Setup (Without Docker)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Kafka locally
# (Use Confluent Community or Docker)

# Start services in separate terminals:

# Terminal 1: Kafka Producer
python producer/kafka_producer.py

# Terminal 2: Spark Streaming (requires Spark installation)
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    processor/spark_stream_processor.py

# Terminal 3: Streamlit Dashboard
streamlit run serving/streamlit_app.py
```

## 📁 Project Structure

```
Real_Time_Ride-Analysis/
├── producer/
│   └── kafka_producer.py           # Generates 100+ events/sec
├── processor/
│   └── spark_stream_processor.py    # Streaming aggregations
├── serving/
│   ├── streamlit_app.py           # Live dashboard
│   └── redis_handler.py           # Cache operations
├── config/
│   ├── schemas.py                 # Avro event schemas
│   └── settings.py                # Config management
├── data_quality/
│   └── validators.py              # Data integrity checks
├── terraform/
│   ├── main.tf                    # AWS infrastructure
│   ├── variables.tf               # Terraform variables
│   └── user_data.sh               # EC2 initialization
├── docker-compose.yml             # Local stack
├── Dockerfile.producer            # Producer container
├── Dockerfile.streamlit           # Dashboard container
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment template
└── README.md                      # This file
```

## ⚙️ Configuration

Copy `.env.example` to `.env` and update values:

```bash
cp .env.example .env
```

Key variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka connection
- `NUM_DRIVERS`: Number of simulated drivers (default: 50)
- `EVENTS_PER_SECOND`: Event generation rate (default: 100)
- `SURGE_THRESHOLD`: Requests triggering surge (default: 100)
- `WINDOW_DURATION_MINUTES`: Aggregation window (default: 5)

## 🧪 Data Quality

The pipeline includes Great Expectations validators:

```python
from data_quality.validators import event_validator

event = {...}
is_valid, errors = event_validator.validate_event(event)

if not is_valid:
    print(f"Validation errors: {errors}")
```

Checks performed:
- Required fields presence
- Latitude/longitude bounds (-90/+90, -180/+180)
- Trip distance validity (0-5000 km)
- Surge multiplier range (0.5-5.0x)
- Timestamp freshness

## ☁️ Cloud Deployment

### AWS with Terraform

```bash
cd terraform

# Initialize Terraform
terraform init

# Plan deployment
terraform plan -var="aws_region=us-east-1"

# Apply infrastructure
terraform apply -var="aws_region=us-east-1"
```

Resources created:
- S3 bucket for Iceberg data lake
- EC2 instance for Spark processor
- CloudWatch logs for monitoring
- IAM roles & policies

## 📊 Streamlit Dashboard

The dashboard provides:

- **Real-time Metrics**: Requests, drivers, surge status by zone
- **Demand Charts**: Bar charts showing distribution
- **Surge Alerts**: Notifications when thresholds exceeded
- **Cache Stats**: Redis performance monitoring
- **System Config**: Current settings and parameters

Access at: `http://localhost:8501`

## 🔍 Monitoring & Debugging

### View Kafka messages

```bash
docker compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic ride_events \
    --from-beginning
```

### Check Redis cache

```bash
docker compose exec redis redis-cli
> KEYS surge:*
> GET surge:manhattan:current
```

### Check Spark streaming

Spark UI available at: `http://localhost:4040`

### Database queries

```bash
docker compose exec postgres psql -U postgres -d ride_analytics
```

## 🎓 Learning Resources

This project demonstrates:

- **Stream Processing**: Kafka, PySpark Structured Streaming
- **Geospatial**: PostGIS, zone-based aggregations
- **Real-time Systems**: Windowing, watermarking, late data handling
- **Data Quality**: Great Expectations
- **Infrastructure**: Docker, Terraform, AWS
- **Data Visualization**: Streamlit, Plotly
- **Cache Optimization**: Redis for sub-ms lookups

## 🚨 Common Issues

### Kafka connection refused
- Ensure Kafka container is healthy: `docker compose ps`
- Check bootstrap servers: `docker compose logs kafka`

### Streamlit not updating
- Verify Redis connection: `docker compose logs streamlit`
- Check producer is running: `docker compose logs producer`

### Out of memory
- Reduce `NUM_DRIVERS` in `.env`
- Reduce `EVENTS_PER_SECOND`
- Increase Docker memory limits

## 📝 Next Steps for Production

- [ ] Implement schema registry for Avro
- [ ] Add Iceberg table format on S3
- [ ] Set up Airflow/Dagster for orchestration
- [ ] Implement alerting (PagerDuty, Slack)
- [ ] Add authentication to Streamlit
- [ ] Set up CI/CD with GitHub Actions
- [ ] Implement dbt transformations for Gold layer
- [ ] Add distributed tracing (Jaeger)

## 📜 License

MIT

## 👤 Author

Built for showcase in Data Science & AI portfolio.

---

**Questions?** Check the documentation or open an issue!
