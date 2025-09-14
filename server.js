require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const path = require('path');

const app = express();
const port = process.env.APP_PORT || 3000;

const pool = new Pool({
  user: process.env.POSTGRES_USER,
  host: process.env.DB_HOST || 'localhost',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.DB_PORT || 5432,
});

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.send(`
    <h1>Latency Dashboard</h1>
    <ul>
      <li><a href="/latency-heatmap">Latency Heatmap</a></li>
      <li><a href="/latency">Latency Time Series</a></li>
    </ul>
  `);
});

app.get('/latency-heatmap', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'heatmap.html'));
});

app.get('/latency', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'latency.html'));
});

app.get('/api/latency', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) as timestamp,
        broker,
        latency_ms
      FROM order_latency
      WHERE DATE(timestamp) = CURRENT_DATE
        AND EXTRACT(hour FROM timestamp) >= 0
        AND EXTRACT(hour FROM timestamp) < 6
      ORDER BY timestamp ASC
    `);

    const processedRows = result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      latency_ms: parseFloat(row.latency_ms)
    }));

    res.json(processedRows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

app.get('/api/latency/timeseries', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) as timestamp,
        broker,
        latency_ms
      FROM order_latency
      WHERE timestamp >= NOW() - INTERVAL '1 hour'
        AND timestamp <= NOW()
      ORDER BY timestamp ASC
    `);

    res.json(result.rows.map(row => ({
      timestamp: parseFloat(row.timestamp),
      broker: row.broker,
      latency_ms: parseFloat(row.latency_ms)
    })));
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Database error' });
  }
});

app.post('/api/latency', async (req, res) => {
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
      if (typeof side !== 'string' || side.length !== 1) {
        return res.status(400).json({
          error: 'side must be a single character (B/S)'
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

    const result = await pool.query(
      'INSERT INTO order_latency (timestamp, broker, latency_ms, symbol, side, price, volume) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *',
      [timestampToUse, broker, latency_ms, symbol, side, price, volume]
    );

    res.status(201).json({
      message: 'Data inserted successfully'
    });
  } catch (err) {
    console.error('Error inserting data:', err);
    res.status(500).json({ error: 'Database error' });
  }
});

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${port}`);
});