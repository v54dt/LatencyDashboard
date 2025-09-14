#!/bin/bash

# Script to load test data into PostgreSQL database
# Usage: ./load_data.sh [data_file]

set -e

# Load environment variables
if [ -f "../../.env" ]; then
    export $(cat ../../.env | grep -v '#' | xargs)
fi

# Default values
DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-5432}
POSTGRES_DB=${POSTGRES_DB:-latency_db}
POSTGRES_USER=${POSTGRES_USER:-admin}

# Use provided data file or default
DATA_FILE=${1:-"../sql/full_day_data.sql"}

echo "Loading data from: $DATA_FILE"
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"

# Check if file exists
if [ ! -f "$DATA_FILE" ]; then
    echo "Error: Data file '$DATA_FILE' not found!"
    exit 1
fi

# Load data into PostgreSQL
PGPASSWORD=$POSTGRES_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -f "$DATA_FILE"

echo "Data loaded successfully!"