-- Latency Dashboard Database Schema

-- Set the database default timezone to UTC+8 (Asia/Taipei).
-- This affects CURRENT_DATE, NOW(), and EXTRACT(hour ...) results.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''Asia/Taipei''', current_database());
END
$$;

CREATE TABLE order_latency (
    id          BIGSERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    broker      VARCHAR(50) NOT NULL,
    latency_ms  DOUBLE PRECISION NOT NULL,
    symbol      VARCHAR(20),
    side        CHAR(1) CHECK (side IN ('B', 'S')),
    price       DOUBLE PRECISION,
    volume      INTEGER
);

CREATE INDEX idx_order_latency_timestamp ON order_latency(timestamp);
CREATE INDEX idx_order_latency_broker    ON order_latency(broker);
CREATE INDEX idx_order_latency_symbol    ON order_latency(symbol);
