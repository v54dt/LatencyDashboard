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
    'order_metrics rows'   AS metric,
    COUNT(*)::text         AS value
FROM order_metrics
UNION ALL
SELECT
    'network_metrics rows',
    COUNT(*)::text
FROM network_metrics
UNION ALL
SELECT
    'unique brokers',
    COUNT(DISTINCT broker)::text
FROM order_metrics
UNION ALL
SELECT
    'success rate',
    CASE
        WHEN COUNT(*) > 0 THEN
            ROUND(100.0 * COUNT(*) FILTER (WHERE outcome = 'success') / COUNT(*), 2)::text || '%'
        ELSE 'n/a'
    END
FROM order_metrics
UNION ALL
SELECT
    'order date range',
    CASE
        WHEN COUNT(*) > 0 THEN
            TO_CHAR(MIN(timestamp), 'YYYY-MM-DD') || ' to ' || TO_CHAR(MAX(timestamp), 'YYYY-MM-DD')
        ELSE 'No data'
    END
FROM order_metrics;
"

echo
echo "=== Recent order_metrics ==="
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "
SELECT timestamp, broker, outcome, total_ms, ack_rtt_ms
FROM order_metrics
ORDER BY timestamp DESC
LIMIT 5;
"

echo
echo "=== Recent network_metrics ==="
PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "
SELECT timestamp, broker, dns_ms, tcp_handshake_ms, tls_handshake_ms, resumption_supported
FROM network_metrics
ORDER BY timestamp DESC
LIMIT 5;
"
