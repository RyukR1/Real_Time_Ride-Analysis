.PHONY: help install validate start stop logs clean

help:
	@echo "🚗 Real-Time Ride Analytics - Makefile Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install      - Install Python dependencies"
	@echo "  make validate     - Validate setup"
	@echo ""
	@echo "Docker:"
	@echo "  make start        - Start Docker Compose stack"
	@echo "  make stop         - Stop Docker Compose stack"
	@echo "  make logs         - View Docker logs"
	@echo "  make clean        - Remove Docker volumes (WARNING: deletes data)"
	@echo ""
	@echo "Development:"
	@echo "  make producer     - Run Kafka producer locally"
	@echo "  make processor    - Run Spark processor locally"
	@echo "  make streamlit    - Run Streamlit dashboard"
	@echo ""
	@echo "Cloud:"
	@echo "  make terraform-plan     - Plan AWS infrastructure"
	@echo "  make terraform-apply    - Apply AWS infrastructure"
	@echo "  make terraform-destroy  - Destroy AWS infrastructure"

install:
	pip install -r requirements.txt

validate:
	python validate_setup.py

start:
	docker compose up -d
	@echo "✓ Services started"
	@echo "  Dashboard: http://localhost:8501"
	@echo "  Kafka: localhost:9092"
	@echo "  PostgreSQL: localhost:5432"
	@echo "  Redis: localhost:6379"

stop:
	docker compose down

logs:
	docker compose logs -f

logs-producer:
	docker compose logs -f producer

logs-streamlit:
	docker compose logs -f streamlit

clean:
	@echo "⚠️  WARNING: This will delete all data!"
	@read -p "Continue? (y/N): " confirm && [ "$$confirm" = "y" ] && docker compose down -v || echo "Cancelled"

producer:
	python producer/kafka_producer.py

processor:
	spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
		processor/spark_stream_processor.py

streamlit:
	streamlit run serving/streamlit_app.py

terraform-plan:
	cd terraform && terraform plan

terraform-apply:
	cd terraform && terraform apply

terraform-destroy:
	cd terraform && terraform destroy

# Docker health check
health:
	@docker compose ps
	@echo ""
	@echo "🔍 Service Health Check:"
	@echo "  Kafka:       docker compose exec kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092"
	@echo "  PostgreSQL:  docker compose exec postgres pg_isready -U postgres"
	@echo "  Redis:       docker compose exec redis redis-cli ping"

# Clean up development artifacts
dev-clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name ".DS_Store" -delete
	rm -rf .pytest_cache .coverage htmlcov

format:
	black producer/ processor/ serving/ config/ data_quality/
	flake8 producer/ processor/ serving/ config/ data_quality/

test:
	pytest -v
