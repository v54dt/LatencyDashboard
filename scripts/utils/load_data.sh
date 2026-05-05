#!/bin/bash

# Load a SQL file into the database.
# Usage: ./load_data.sh <data_file>

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

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <data_file>"
    exit 1
fi

DATA_FILE="$1"

if [ ! -f "$DATA_FILE" ]; then
    echo "Error: Data file '$DATA_FILE' not found!"
    exit 1
fi

echo "Loading data from: $DATA_FILE"
echo "Database: $POSTGRES_DB on $DB_HOST:$DB_PORT"
echo "User: $POSTGRES_USER"

PGPASSWORD="$POSTGRES_PASSWORD" psql \
    -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -f "$DATA_FILE"

echo "Data loaded successfully!"
