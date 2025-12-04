.PHONY: help install test run clean docker-up docker-down docker-logs setup

help:
	@echo "Data Engineering Project - Makefile Commands"
	@echo ""
	@echo "  make setup        - Set up the project (install dependencies)"
	@echo "  make install      - Install Python dependencies"
	@echo "  make test         - Run tests"
	@echo "  make test-cov     - Run tests with coverage"
	@echo "  make run          - Run the pipeline with sample data"
	@echo "  make docker-up    - Start Docker services"
	@echo "  make docker-down  - Stop Docker services"
	@echo "  make docker-logs  - View Docker logs"
	@echo "  make clean        - Clean temporary files"
	@echo "  make lint         - Run linters"

setup: install
	@echo "Setting up project..."
	@mkdir -p data/raw data/output logs
	@echo "Project setup complete!"

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt

test:
	@echo "Running tests..."
	pytest

test-cov:
	@echo "Running tests with coverage..."
	pytest --cov=src --cov-report=html --cov-report=term

run:
	@echo "Running pipeline..."
	python main.py --source sample --output file

docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d
	@echo "Docker services started!"

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs:
	@echo "Viewing Docker logs..."
	docker-compose logs -f

clean:
	@echo "Cleaning temporary files..."
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -r {} + 2>/dev/null || true
	@echo "Clean complete!"

lint:
	@echo "Running linters..."
	flake8 src tests main.py
	black --check src tests main.py
	mypy src

format:
	@echo "Formatting code..."
	black src tests main.py

