#!/bin/bash

# Script to check database status and show basic statistics
# Usage: ./db_status.sh

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

echo "=== Database Status ==="
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"
echo

# Check if database is accessible
if ! PGPASSWORD=$POSTGRES_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Cannot connect to database!"
    exit 1
fi

echo "✅ Database connection successful"
echo

# Get basic statistics
echo "=== Table Statistics ==="
PGPASSWORD=$POSTGRES_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM order_latency
UNION ALL
SELECT
    'Unique Brokers',
    COUNT(DISTINCT broker)
FROM order_latency
UNION ALL
SELECT
    'Date Range',
    CASE
        WHEN COUNT(*) > 0 THEN
            TO_CHAR(MIN(timestamp), 'YYYY-MM-DD') || ' to ' || TO_CHAR(MAX(timestamp), 'YYYY-MM-DD')
        ELSE 'No data'
    END
FROM order_latency;
"

echo
echo "=== Recent Data Sample ==="
PGPASSWORD=$POSTGRES_PASSWORD psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -c "
SELECT
    timestamp,
    broker,
    latency_ms,
    symbol
FROM order_latency
ORDER BY timestamp DESC
LIMIT 5;
"