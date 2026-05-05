#!/bin/bash

# Clear all rows from the order_latency table.
# Usage: ./clear_data.sh

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

echo "Clearing data from order_latency table..."
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"

read -r -p "Are you sure you want to delete all data? (y/N): " REPLY
if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
    echo "Operation cancelled."
    exit 1
fi

PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -c "TRUNCATE TABLE order_latency;"

echo "All data cleared successfully!"
