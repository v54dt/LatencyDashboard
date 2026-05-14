-- Latency Dashboard Database Schema

-- Set the database default timezone to UTC+8 (Asia/Taipei).
-- This affects CURRENT_DATE, NOW(), and EXTRACT(hour ...) results.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''Asia/Taipei''', current_database());
END
$$;

-- ============================================================================
-- Order metric
-- ============================================================================
CREATE TABLE IF NOT EXISTS order_metrics (
  id                          BIGSERIAL PRIMARY KEY,
  timestamp                   TIMESTAMPTZ      NOT NULL,
  iteration_id                INTEGER          NOT NULL,
  broker                      TEXT             NOT NULL,

  outcome                     TEXT             NOT NULL
    CHECK (outcome IN ('success', 'ack_timeout', 'submit_error',
                       'cancel_timeout', 'cancel_error')),
  error_message               TEXT,

  total_ms                    DOUBLE PRECISION,
  sdk_local_ms                DOUBLE PRECISION,
  ack_rtt_ms                  DOUBLE PRECISION,
  cancel_rtt_ms               DOUBLE PRECISION,

  minor_faults                INTEGER,
  major_faults                INTEGER,
  voluntary_ctxt_switches     INTEGER,
  involuntary_ctxt_switches   INTEGER,

  tcp_rtt_us                  INTEGER,
  tcp_rttvar_us               INTEGER,
  tcp_snd_cwnd                INTEGER,
  tcp_retrans                 INTEGER
);

CREATE INDEX IF NOT EXISTS idx_order_metrics_broker_ts
  ON order_metrics (broker, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_metrics_iter
  ON order_metrics (broker, iteration_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_order_metrics_outcome
  ON order_metrics (outcome) WHERE outcome <> 'success';

-- ============================================================================
-- Network metric
-- ============================================================================
CREATE TABLE IF NOT EXISTS network_metrics (
  id                          BIGSERIAL PRIMARY KEY,
  timestamp                   TIMESTAMPTZ      NOT NULL,
  iteration_id                INTEGER          NOT NULL,
  broker                      TEXT             NOT NULL,

  dns_ms                      DOUBLE PRECISION,
  tcp_handshake_ms            DOUBLE PRECISION,
  tls_handshake_ms            DOUBLE PRECISION,

  tls_handshake_resumed_ms    DOUBLE PRECISION,
  resumption_supported        BOOLEAN,

  error                       TEXT
);

CREATE INDEX IF NOT EXISTS idx_network_metrics_broker_ts
  ON network_metrics (broker, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_network_metrics_iter
  ON network_metrics (broker, iteration_id, timestamp);

-- ============================================================================
-- Joined view: order ↔ closest network probe within ±1 minute.
-- iteration_id is unique only within a process run and resets on restart, so
-- the time window guards against id collisions across processes.
-- ============================================================================
CREATE OR REPLACE VIEW iteration_view AS
SELECT
  o.broker,
  o.iteration_id,
  o.timestamp                      AS order_ts,
  n.timestamp                      AS network_ts,
  o.outcome,
  o.error_message,
  o.total_ms,
  o.sdk_local_ms,
  o.ack_rtt_ms,
  o.cancel_rtt_ms,
  o.minor_faults,
  o.major_faults,
  o.voluntary_ctxt_switches,
  o.involuntary_ctxt_switches,
  o.tcp_rtt_us,
  o.tcp_rttvar_us,
  o.tcp_snd_cwnd,
  o.tcp_retrans,
  n.dns_ms,
  n.tcp_handshake_ms,
  n.tls_handshake_ms,
  n.tls_handshake_resumed_ms,
  n.resumption_supported,
  n.error                          AS network_error
FROM order_metrics o
LEFT JOIN LATERAL (
  SELECT *
  FROM network_metrics nm
  WHERE nm.broker = o.broker
    AND nm.iteration_id = o.iteration_id
    AND nm.timestamp BETWEEN o.timestamp - INTERVAL '1 minute'
                         AND o.timestamp + INTERVAL '1 minute'
  ORDER BY ABS(EXTRACT(EPOCH FROM (nm.timestamp - o.timestamp)))
  LIMIT 1
) n ON TRUE;
