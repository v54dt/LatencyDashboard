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
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.send(`
    <h1>Latency Dashboard API</h1>
    <p>Server is running successfully!</p>
    <ul>
      <li><a href="/dashboard">Dashboard</a></li>
      <li><a href="/latency-heatmap">Latency Heatmap</a></li>
      <li><a href="/api/latency">API Latency Data</a></li>
    </ul>
  `);
});

app.get('/latency-heatmap', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'heatmap.html'));
});

app.get('/api/latency', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        EXTRACT(EPOCH FROM date_trunc('second', timestamp)) as timestamp,
        broker,
        latency_ms
      FROM order_latency
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

app.listen(port, '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${port}`);
});