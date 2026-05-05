#!/bin/bash

# Print database connectivity status and basic table statistics.
# Usage: ./db_status.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../../.env"

if [ -f "$ENV_FILE" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
fi

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
POSTGRES_DB="${POSTGRES_DB:-latency_db}"
POSTGRES_USER="${POSTGRES_USER:-admin}"

echo "=== Database Status ==="
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"
echo

if ! PGPASSWORD="$POSTGRES_PASSWORD" psql \
        -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
        -c "SELECT 1;" > /dev/null 2>&1; then
    echo "Cannot connect to database!"
    exit 1
fi

echo "Database connection successful"
echo

echo "=== Table Statistics ==="
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "
SELECT
    'Total Records' AS metric,
    COUNT(*)::text AS value
FROM order_latency
UNION ALL
SELECT
    'Unique Brokers',
    COUNT(DISTINCT broker)::text
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
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "
SELECT timestamp, broker, latency_ms, symbol
FROM order_latency
ORDER BY timestamp DESC
LIMIT 5;
"
