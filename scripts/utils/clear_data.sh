#!/bin/bash

# Script to clear all data from order_latency table
# Usage: ./clear_data.sh

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

echo "Clearing data from order_latency table..."
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"

# Confirm action
read -p "Are you sure you want to delete all data? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Operation cancelled."
    exit 1
fi

# Clear data
PGPASSWORD=$POSTGRES_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "TRUNCATE TABLE order_latency;"

echo "All data cleared successfully!"