.PHONY: help install install-dev test lint format clean run docker-build docker-run setup

# Default target
help:
	@echo "Available commands:"
	@echo "  install      - Install production dependencies"
	@echo "  install-dev  - Install development dependencies"
	@echo "  test         - Run tests"
	@echo "  lint         - Run linting"
	@echo "  format       - Format code"
	@echo "  clean        - Clean up temporary files"
	@echo "  run          - Run the application"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run with Docker Compose"
	@echo "  setup        - Run initial setup"

# Installation
install:
	pip install -r requirements.txt

install-dev:
	pip install -r requirements.txt -r requirements-dev.txt

# Testing
test:
	python -m pytest tests/ -v

test-cov:
	python -m pytest tests/ --cov=src --cov-report=html --cov-report=term

# Code quality
lint:
	flake8 src/ tests/
	mypy src/

format:
	black src/ tests/
	isort src/ tests/

# Cleanup
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

# Application
run:
	python main.py

# Docker
docker-build:
	docker build -f docker/Dockerfile -t pyspark-transaction-processor .

docker-run:
	docker-compose -f docker/docker-compose.yml up -d

docker-stop:
	docker-compose -f docker/docker-compose.yml down

# Setup
setup:
	chmod +x setup/setup.sh
	./setup/setup.sh

# Development workflow
dev-setup: install-dev setup
	pre-commit install

# CI/CD helpers
ci-test: install-dev lint test

# Database operations
db-setup:
	python -c "from src.utils.postgres_utils import PostgresUtils; PostgresUtils().create_tables()"

# Validation
validate-config:
	python -c "from config.config import Config; Config.validate(); print('✅ Configuration valid')"

validate-connections:
	python -c "from src.utils.postgres_utils import PostgresUtils; PostgresUtils().get_connection().close(); print('✅ Database connected')"
	python -c "from src.utils.s3_utils import S3Utils; S3Utils().list_files(); print('✅ S3 connected')"

# Deployment helpers
deploy-local: setup validate-config validate-connections run

deploy-docker: docker-build docker-run