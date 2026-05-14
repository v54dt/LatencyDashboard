require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');

const readApp = express();
const writeApp = express();

const readPort = process.env.APP_PORT || 3000;
const writePort = process.env.WRITE_PORT || 3001;

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: process.env.DB_HOST || 'localhost',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.DB_PORT || 5432,
});

// Without this, an idle-client error tears down the whole Node process.
pool.on('error', (err) => {
  console.error('Unexpected pg pool error:', err);
});

// Force every checked-out connection into UTC+8 so EXTRACT/CURRENT_DATE/NOW()
// behave identically to the values shown in the dashboard.
pool.on('connect', (client) => {
  client.query("SET TIME ZONE 'Asia/Taipei'").catch((err) => {
    console.error('Failed to set session timezone:', err);
  });
});

const JSON_LIMIT = '32kb';

readApp.use(cors());
readApp.use(express.json({ limit: JSON_LIMIT }));
readApp.use(express.static('public'));

writeApp.use(cors());
writeApp.use(express.json({ limit: JSON_LIMIT }));

// READ APP ROUTES
readApp.get('/', (req, res) => {
  res.send(`
    <h1>Latency Dashboard</h1>
    <ul>
      <li><a href="/latency-heatmap">Latency Heatmap (08:00–14:00)</a></li>
      <li><a href="/latency">Latency Time Series (08:00–14:00)</a></li>
      <li><a href="/latest">Latest 1 Hour</a></li>
    </ul>
  `);
});

readApp.get('/latency-heatmap', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'heatmap.html'));
});

readApp.get('/latency', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'latency.html'));
});

readApp.get('/latest', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'latest.html'));
});

// Heatmap: today's 08:00–14:00 trading window in Asia/Taipei.
// With session TZ set to Asia/Taipei, CURRENT_DATE is local and we can
// build the window directly without DATE()/EXTRACT() tricks that defeat
// the timestamp index.
readApp.get('/api/order-metrics', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) AS timestamp,
        broker,
        total_ms
      FROM order_metrics
      WHERE timestamp >= CURRENT_DATE + TIME '08:00'
        AND timestamp <  CURRENT_DATE + TIME '14:00'
        AND total_ms IS NOT NULL
      ORDER BY timestamp ASC
    `);

    res.json(result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      total_ms: parseFloat(row.total_ms),
    })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

readApp.get('/api/order-metrics/timeseries', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) AS timestamp,
        broker,
        total_ms
      FROM order_metrics
      WHERE timestamp >= NOW() - INTERVAL '1 hour'
        AND total_ms IS NOT NULL
      ORDER BY timestamp ASC
    `);

    res.json(result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      total_ms: parseFloat(row.total_ms),
    })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

// WRITE APP ROUTES
// No JS-side type checking — DB NOT NULL / CHECK constraints will surface
// bad payloads as 400s.
writeApp.post('/api/order-metrics', async (req, res) => {
  try {
    const b = req.body || {};
    const ts = b.timestamp ? new Date(b.timestamp) : new Date();

    await pool.query(
      `INSERT INTO order_metrics (
         timestamp, iteration_id, broker,
         outcome, error_message,
         total_ms, sdk_local_ms, ack_rtt_ms, cancel_rtt_ms,
         minor_faults, major_faults,
         voluntary_ctxt_switches, involuntary_ctxt_switches,
         tcp_rtt_us, tcp_rttvar_us, tcp_snd_cwnd, tcp_retrans
       ) VALUES (
         $1, $2, $3,
         $4, $5,
         $6, $7, $8, $9,
         $10, $11,
         $12, $13,
         $14, $15, $16, $17
       )`,
      [
        ts, b.iteration_id, b.broker,
        b.outcome, b.error_message ?? null,
        b.total_ms ?? null, b.sdk_local_ms ?? null,
        b.ack_rtt_ms ?? null, b.cancel_rtt_ms ?? null,
        b.minor_faults ?? null, b.major_faults ?? null,
        b.voluntary_ctxt_switches ?? null, b.involuntary_ctxt_switches ?? null,
        b.tcp_rtt_us ?? null, b.tcp_rttvar_us ?? null,
        b.tcp_snd_cwnd ?? null, b.tcp_retrans ?? null,
      ]
    );

    res.status(201).json({ message: 'order_metrics inserted' });
  } catch (err) {
    if (err.code === '23502' || err.code === '23514') {
      return res.status(400).json({ error: err.message });
    }
    console.error('Error inserting order_metrics:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

writeApp.post('/api/network-metrics', async (req, res) => {
  try {
    const b = req.body || {};
    const ts = b.timestamp ? new Date(b.timestamp) : new Date();

    await pool.query(
      `INSERT INTO network_metrics (
         timestamp, iteration_id, broker,
         dns_ms, tcp_handshake_ms, tls_handshake_ms,
         tls_handshake_resumed_ms, resumption_supported,
         error
       ) VALUES (
         $1, $2, $3,
         $4, $5, $6,
         $7, $8,
         $9
       )`,
      [
        ts, b.iteration_id, b.broker,
        b.dns_ms ?? null, b.tcp_handshake_ms ?? null, b.tls_handshake_ms ?? null,
        b.tls_handshake_resumed_ms ?? null, b.resumption_supported ?? null,
        b.error ?? null,
      ]
    );

    res.status(201).json({ message: 'network_metrics inserted' });
  } catch (err) {
    if (err.code === '23502' || err.code === '23514') {
      return res.status(400).json({ error: err.message });
    }
    console.error('Error inserting network_metrics:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

const readServer = readApp.listen(readPort, '0.0.0.0', () => {
  console.log(`Read server (GET) listening on http://0.0.0.0:${readPort}`);
});

const writeServer = writeApp.listen(writePort, '0.0.0.0', () => {
  console.log(`Write server (POST) listening on http://0.0.0.0:${writePort}`);
});

console.log(`Dashboard:   http://localhost:${readPort}`);
console.log(`Write APIs:  http://localhost:${writePort}/api/order-metrics`);
console.log(`             http://localhost:${writePort}/api/network-metrics`);

function shutdown(signal) {
  console.log(`Received ${signal}, draining connections...`);
  let pending = 2;
  const done = () => {
    if (--pending === 0) {
      pool.end().then(() => process.exit(0)).catch(() => process.exit(1));
    }
  };
  readServer.close(done);
  writeServer.close(done);
  setTimeout(() => {
    console.error('Shutdown timed out, forcing exit');
    process.exit(1);
  }, 10000).unref();
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
