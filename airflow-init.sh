#!/bin/bash

# YouTube Analytics Airflow Initialization Script
# This script sets up Airflow with uv and initializes the database

set -e

echo "Initializing YouTube Analytics Airflow Environment..."

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL..."
until docker exec youtube_postgres pg_isready -U airflow -d youtube_analytics; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done

echo "PostgreSQL is ready!"

# Install uv in Airflow containers
echo "Installing uv in Airflow containers..."
docker exec youtube_airflow_webserver pip install uv
docker exec youtube_airflow_scheduler pip install uv
docker exec youtube_airflow_worker pip install uv

# Install Python dependencies using uv
echo "Installing Python dependencies..."
docker exec youtube_airflow_webserver uv pip install -r /opt/airflow/pyproject.toml
docker exec youtube_airflow_scheduler uv pip install -r /opt/airflow/pyproject.toml
docker exec youtube_airflow_worker uv pip install -r /opt/airflow/pyproject.toml

# Initialize Airflow database
echo "Initializing Airflow database..."
docker exec youtube_airflow_webserver airflow db init

# Create Airflow user
echo "Creating Airflow user..."
docker exec youtube_airflow_webserver airflow users create \
    --username airflow \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

echo "Airflow initialization completed!"
echo "Access Airflow at: http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "Access Grafana at: http://localhost:3000"
echo "Username: admin"
echo "Password: admin"