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
readApp.get('/api/latency', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) AS timestamp,
        broker,
        latency_ms
      FROM order_latency
      WHERE timestamp >= CURRENT_DATE + TIME '08:00'
        AND timestamp <  CURRENT_DATE + TIME '14:00'
      ORDER BY timestamp ASC
    `);

    res.json(result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      latency_ms: parseFloat(row.latency_ms),
    })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

readApp.get('/api/latency/timeseries', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) AS timestamp,
        broker,
        latency_ms
      FROM order_latency
      WHERE timestamp >= NOW() - INTERVAL '1 hour'
      ORDER BY timestamp ASC
    `);

    res.json(result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      latency_ms: parseFloat(row.latency_ms),
    })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

// WRITE APP ROUTES
writeApp.post('/api/latency', async (req, res) => {
  try {
    const { broker, latency_ms, timestamp, symbol, side, price, volume } = req.body;

    if (!broker || latency_ms === undefined) {
      return res.status(400).json({
        error: 'Missing required fields: broker and latency_ms are required'
      });
    }

    if (typeof broker !== 'string' || broker.length === 0 || broker.length > 50) {
      return res.status(400).json({
        error: 'broker must be a non-empty string with max 50 characters'
      });
    }

    if (typeof latency_ms !== 'number' || latency_ms < 0 || !isFinite(latency_ms)) {
      return res.status(400).json({
        error: 'latency_ms must be a non-negative finite number'
      });
    }

    const timestampToUse = timestamp ? new Date(timestamp) : new Date();
    if (timestamp && isNaN(timestampToUse.getTime())) {
      return res.status(400).json({
        error: 'Invalid timestamp format'
      });
    }

    if (symbol !== undefined && symbol !== null) {
      if (typeof symbol !== 'string' || symbol.length > 20) {
        return res.status(400).json({
          error: 'symbol must be a string with max 20 characters'
        });
      }
    }

    if (side !== undefined && side !== null) {
      if (side !== 'B' && side !== 'S') {
        return res.status(400).json({
          error: "side must be 'B' or 'S'"
        });
      }
    }

    if (price !== undefined && price !== null) {
      if (typeof price !== 'number' || price < 0 || !isFinite(price)) {
        return res.status(400).json({
          error: 'price must be a non-negative finite number'
        });
      }
    }

    if (volume !== undefined && volume !== null) {
      if (!Number.isInteger(volume) || volume < 0) {
        return res.status(400).json({
          error: 'volume must be a non-negative integer'
        });
      }
    }

    await pool.query(
      'INSERT INTO order_latency (timestamp, broker, latency_ms, symbol, side, price, volume) VALUES ($1, $2, $3, $4, $5, $6, $7)',
      [timestampToUse, broker, latency_ms, symbol, side, price, volume]
    );

    res.status(201).json({ message: 'Data inserted successfully' });
  } catch (err) {
    console.error('Error inserting data:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

const readServer = readApp.listen(readPort, '0.0.0.0', () => {
  console.log(`Read server (GET) listening on http://0.0.0.0:${readPort}`);
});

const writeServer = writeApp.listen(writePort, '0.0.0.0', () => {
  console.log(`Write server (POST) listening on http://0.0.0.0:${writePort}`);
});

console.log(`Dashboard:  http://localhost:${readPort}`);
console.log(`Write API:  http://localhost:${writePort}/api/latency`);

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
