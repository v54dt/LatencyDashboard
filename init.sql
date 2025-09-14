-- Latency Dashboard Database Schema
-- Create table for order latency data

CREATE TABLE order_latency (
    timestamp TIMESTAMP,
    broker VARCHAR(50) NOT NULL,
    latency_ms FLOAT NOT NULL,
    symbol VARCHAR(20),
    side VARCHAR(1),
    price FLOAT,
    volume INTEGER
);

-- Create index for better query performance
CREATE INDEX idx_order_latency_timestamp ON order_latency(timestamp);
CREATE INDEX idx_order_latency_broker ON order_latency(broker);
CREATE INDEX idx_order_latency_symbol ON order_latency(symbol);