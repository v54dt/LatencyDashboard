#!/usr/bin/env node

// Generate synthetic order_metrics + network_metrics and insert directly to DB.
const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });
const { Pool } = require('pg');

const brokers = ['BrokerA', 'BrokerB', 'BrokerC'];

// Outcome mix — mostly success, with a sprinkle of failures.
const failureOutcomes = [
    'ack_timeout', 'submit_error', 'cancel_timeout', 'cancel_error',
];
const FAILURE_RATE = 0.02;

const pool = new Pool({
    user: process.env.POSTGRES_USER,
    host: process.env.DB_HOST || 'localhost',
    database: process.env.POSTGRES_DB,
    password: process.env.POSTGRES_PASSWORD,
    port: process.env.DB_PORT || 5432,
});

function gaussian(mean, stddev) {
    const u1 = Math.random();
    const u2 = Math.random();
    const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
    return mean + z * stddev;
}

function randInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pickFailure() {
    return failureOutcomes[Math.floor(Math.random() * failureOutcomes.length)];
}

// Build one order_metrics row.
function makeOrderRow(timestampStr, broker, iterationId, brokerOffset) {
    const isFailure = Math.random() < FAILURE_RATE;
    const outcome = isFailure ? pickFailure() : 'success';
    const errorMsg = isFailure ? `synthetic ${outcome}` : null;

    // Generate plausible values; mask per outcome below to match client.
    const sdkLocal = Math.max(0.05, gaussian(0.5, 0.2));
    const ackRtt = Math.max(1, gaussian(25 + brokerOffset, 5));
    const cancelRtt = Math.max(1, gaussian(20 + brokerOffset, 4));
    const total = ackRtt + sdkLocal + cancelRtt + Math.max(0, gaussian(2, 1));
    const rusage = [randInt(0, 10), 0, randInt(0, 5), randInt(0, 3)];
    const tcp = [
        Math.round(ackRtt * 1000 * 0.85),  // kernel rtt slightly below ack_rtt
        randInt(500, 5000),
        randInt(10, 100),
        randInt(0, 2),
    ];
    const nulls4 = [null, null, null, null];

    // Null patterns per outcome — matches client's actual emit behavior:
    //   success        : all valid
    //   ack_timeout    : everything null (submit cb never fired)
    //   submit_error   : sdk_local valid + rusage; ack/total/cancel null; tcp null
    //   cancel_timeout : submit succeeded so timing/rusage/tcp valid; cancel_rtt null
    //   cancel_error   : cancel cb fired with success=false; everything valid
    let total_ms, sdk_local_ms, ack_rtt_ms, cancel_rtt_ms;
    let rusageRow, tcpRow;
    switch (outcome) {
        case 'ack_timeout':
            total_ms = sdk_local_ms = ack_rtt_ms = cancel_rtt_ms = null;
            rusageRow = nulls4;
            tcpRow = nulls4;
            break;
        case 'submit_error':
            sdk_local_ms = sdkLocal;
            total_ms = ack_rtt_ms = cancel_rtt_ms = null;
            rusageRow = rusage;
            tcpRow = nulls4;
            break;
        case 'cancel_timeout':
            total_ms = total;
            sdk_local_ms = sdkLocal;
            ack_rtt_ms = ackRtt;
            cancel_rtt_ms = null;
            rusageRow = rusage;
            tcpRow = tcp;
            break;
        case 'cancel_error':
        case 'success':
        default:
            total_ms = total;
            sdk_local_ms = sdkLocal;
            ack_rtt_ms = ackRtt;
            cancel_rtt_ms = cancelRtt;
            rusageRow = rusage;
            tcpRow = tcp;
            break;
    }

    return [
        timestampStr, iterationId, broker,
        outcome, errorMsg,
        total_ms, sdk_local_ms, ack_rtt_ms, cancel_rtt_ms,
        ...rusageRow,
        ...tcpRow,
    ];
}

// Build one network_metrics row.
function makeNetworkRow(timestampStr, broker, iterationId) {
    const dns = Math.max(0.1, gaussian(2, 1));
    const tcp = Math.max(0.5, gaussian(15, 3));
    const tlsCold = Math.max(5, gaussian(40, 8));
    const tlsWarm = Math.max(1, gaussian(8, 2));
    const resumed = Math.random() < 0.7;

    return [
        timestampStr, iterationId, broker,
        dns, tcp, tlsCold,
        tlsWarm, resumed,
        null,
    ];
}

async function generateAndInsertData() {
    const client = await pool.connect();

    try {
        console.log('Starting synthetic data generation...');

        const hours = parseInt(process.argv[2]) || 24;
        const startDate = process.argv[3] || '2025-09-14T00:00:00.000Z';

        const startTime = new Date(startDate);
        const orderIntervalMs = 5000;     // one order per broker every 5s
        const networkIntervalMs = 60000;  // one network probe per broker per minute
        const totalOrdersPerBroker = Math.floor((hours * 60 * 60 * 1000) / orderIntervalMs);
        const totalNetworkPerBroker = Math.floor((hours * 60 * 60 * 1000) / networkIntervalMs);

        console.log(
            `Generating ${totalOrdersPerBroker} order rows and ` +
            `${totalNetworkPerBroker} network rows per broker ` +
            `(${brokers.length} brokers) over ${hours}h from ${startTime.toISOString()}`
        );

        // Per-broker iteration counters — iteration_id is unique within a
        // simulated process run (i.e., this whole generator invocation).
        const orderRows = [];
        const networkRows = [];

        brokers.forEach((broker, brokerIdx) => {
            const brokerOffset = brokerIdx * 2; // slight per-broker baseline shift
            let orderIter = 0;
            let netIter = 0;

            for (let i = 0; i < totalOrdersPerBroker; i++) {
                const ts = new Date(startTime.getTime() + i * orderIntervalMs);
                const micros = String(randInt(0, 999999)).padStart(6, '0');
                const tsStr = ts.toISOString().replace(/\.\d{3}Z$/, `.${micros}Z`);
                orderRows.push(makeOrderRow(tsStr, broker, orderIter++, brokerOffset));
            }

            for (let i = 0; i < totalNetworkPerBroker; i++) {
                const ts = new Date(startTime.getTime() + i * networkIntervalMs);
                const micros = String(randInt(0, 999999)).padStart(6, '0');
                const tsStr = ts.toISOString().replace(/\.\d{3}Z$/, `.${micros}Z`);
                // Align network iteration_id with the order iteration_id that
                // falls within the same minute, so the iteration_view join finds
                // it. With 12 orders per minute the network probe matches
                // orderIter = i * 12.
                networkRows.push(makeNetworkRow(tsStr, broker, i * 12));
                netIter++;
            }
        });

        console.log(`Inserting ${orderRows.length} order_metrics rows...`);
        await batchInsert(client,
            `INSERT INTO order_metrics (
               timestamp, iteration_id, broker,
               outcome, error_message,
               total_ms, sdk_local_ms, ack_rtt_ms, cancel_rtt_ms,
               minor_faults, major_faults,
               voluntary_ctxt_switches, involuntary_ctxt_switches,
               tcp_rtt_us, tcp_rttvar_us, tcp_snd_cwnd, tcp_retrans
             ) VALUES `,
            orderRows, 17);

        console.log(`Inserting ${networkRows.length} network_metrics rows...`);
        await batchInsert(client,
            `INSERT INTO network_metrics (
               timestamp, iteration_id, broker,
               dns_ms, tcp_handshake_ms, tls_handshake_ms,
               tls_handshake_resumed_ms, resumption_supported,
               error
             ) VALUES `,
            networkRows, 9);

        const summary = await client.query(`
            SELECT
                (SELECT COUNT(*) FROM order_metrics)   AS order_rows,
                (SELECT COUNT(*) FROM network_metrics) AS network_rows,
                (SELECT COUNT(DISTINCT broker) FROM order_metrics) AS unique_brokers,
                (SELECT MIN(timestamp) FROM order_metrics) AS earliest,
                (SELECT MAX(timestamp) FROM order_metrics) AS latest
        `);
        const s = summary.rows[0];
        console.log('');
        console.log('Database summary:');
        console.log(`  order_metrics rows:   ${s.order_rows}`);
        console.log(`  network_metrics rows: ${s.network_rows}`);
        console.log(`  unique brokers:       ${s.unique_brokers}`);
        console.log(`  range:                ${s.earliest} → ${s.latest}`);
    } catch (err) {
        console.error('Error:', err.message);
        process.exit(1);
    } finally {
        client.release();
        await pool.end();
    }
}

async function batchInsert(client, sqlPrefix, rows, cols) {
    const batchSize = 1000;
    let inserted = 0;
    for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const placeholders = batch.map((_, idx) => {
            const base = idx * cols;
            const parts = [];
            for (let c = 1; c <= cols; c++) parts.push(`$${base + c}`);
            return `(${parts.join(', ')})`;
        }).join(', ');
        const values = batch.flat();
        await client.query(sqlPrefix + placeholders, values);
        inserted += batch.length;
        if (inserted % 5000 === 0) {
            console.log(`  inserted ${inserted}/${rows.length}`);
        }
    }
}

if (require.main === module) {
    console.log('Latency Dashboard - synthetic data generator');
    console.log('Usage: node generate_data.js [hours] [start_date]');
    console.log('Example: node generate_data.js 6 "2025-09-14T08:00:00.000Z"');
    console.log('');

    generateAndInsertData().catch(console.error);
}
