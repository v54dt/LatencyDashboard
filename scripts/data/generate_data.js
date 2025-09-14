#!/usr/bin/env node

// Generate fake data and insert directly to database
require('dotenv').config({ path: '../../.env' });
const { Pool } = require('pg');

const brokers = ['BrokerA', 'BrokerB', 'BrokerC'];
const symbols = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN', 'META', 'GOOGL', 'NFLX'];
const sides = ['B', 'S'];

// Database connection
const pool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.DB_HOST || 'localhost',
    database: process.env.POSTGRES_DB,
    password: process.env.POSTGRES_PASSWORD,
    port: process.env.DB_PORT || 5432,
});

async function generateAndInsertData() {
    const client = await pool.connect();

    try {
        console.log('🚀 Starting fake data generation...');

        // Get command line arguments
        const hours = parseInt(process.argv[2]) || 24;
        const startDate = process.argv[3] || '2025-09-14T00:00:00.000Z';

        const records = [];
        const startTime = new Date(startDate);
        const totalRecords = hours * 60 * 12; // Every 5 seconds

        console.log(`📊 Generating ${totalRecords} records for ${hours} hours starting from ${startTime.toISOString()}`);

        for (let i = 0; i < totalRecords; i++) {
            const timestamp = new Date(startTime.getTime() + i * 5000); // 5 seconds interval
            const broker = brokers[Math.floor(Math.random() * brokers.length)];
            const symbol = symbols[Math.floor(Math.random() * symbols.length)];
            const side = sides[Math.floor(Math.random() * sides.length)];

            // Normal distribution with mean=30ms, stddev=5ms
            const u1 = Math.random();
            const u2 = Math.random();
            const z0 = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
            const latency = Math.max(1, 30 + z0 * 5);

            const price = Math.random() * 500 + 100;
            const volume = Math.floor(Math.random() * 500 + 100);

            const microseconds = String(Math.floor(Math.random() * 1000000)).padStart(6, '0');
            const timestampStr = timestamp.toISOString().replace('000Z', microseconds + 'Z');

            records.push([timestampStr, broker, latency, symbol, side, price, volume]);
        }

        console.log('💾 Inserting data into database...');

        // Batch insert for better performance
        const batchSize = 1000;
        let insertedCount = 0;

        for (let i = 0; i < records.length; i += batchSize) {
            const batch = records.slice(i, i + batchSize);
            const placeholders = batch.map((_, idx) => {
                const base = idx * 7;
                return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`;
            }).join(', ');

            const values = batch.flat();
            const query = `INSERT INTO order_latency (timestamp, broker, latency_ms, symbol, side, price, volume) VALUES ${placeholders}`;

            await client.query(query, values);
            insertedCount += batch.length;

            if (insertedCount % 5000 === 0) {
                console.log(`   📈 Inserted ${insertedCount}/${totalRecords} records...`);
            }
        }

        console.log(`✅ Successfully inserted ${insertedCount} records!`);

        // Show summary
        const result = await client.query(`
            SELECT
                COUNT(*) as total_records,
                MIN(timestamp) as earliest,
                MAX(timestamp) as latest,
                COUNT(DISTINCT broker) as unique_brokers
            FROM order_latency
        `);

        console.log('\n📊 Database Summary:');
        console.log(`   Total Records: ${result.rows[0].total_records}`);
        console.log(`   Date Range: ${result.rows[0].earliest} to ${result.rows[0].latest}`);
        console.log(`   Unique Brokers: ${result.rows[0].unique_brokers}`);

    } catch (err) {
        console.error('❌ Error:', err.message);
        process.exit(1);
    } finally {
        client.release();
        await pool.end();
    }
}

// Check if this script is run directly
if (require.main === module) {
    console.log('🔧 Latency Dashboard - Fake Data Generator');
    console.log('Usage: node generate_data.js [hours] [start_date]');
    console.log('Example: node generate_data.js 6 "2025-09-14T08:00:00.000Z"');
    console.log('');

    generateAndInsertData().catch(console.error);
}