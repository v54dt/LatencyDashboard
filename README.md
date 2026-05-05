# 📊 Latency Dashboard

A modern web application for visualizing order execution latency across different brokers using advanced heatmap visualization. Built with Node.js, PostgreSQL, and uPlot.

![Dashboard Preview](Demo.png)

## 🚀 Features

- **📈 Interactive Heatmap**: Real-time latency visualization using uPlot aggregated heatmap
- **📉 Time-Series View**: Per-broker latency lines for the most recent hour
- **🏢 Broker Analysis**: Compare latency performance across multiple brokers
- **⏰ Time-based Filtering**: Focus on trading hours (08:00-14:00 UTC+8)
- **📊 Statistical Insights**: Average, maximum, and 99th percentile latency metrics
- **✏️ HTTP Write API**: POST `/api/latency` on the dedicated write port for ingestion
- **🔄 Auto-refresh**: Data updates every 30 seconds

## 🛠️ Tech Stack

- **Backend**: Node.js + Express
- **Database**: PostgreSQL with optimized indexes
- **Frontend**: Pure JavaScript + uPlot visualization library
- **Containerization**: Docker + Docker Compose
- **Environment**: Environment variable configuration

## 📁 Project Structure

```
LatencyDashboard/
├── 🐳 Docker Configuration
│   ├── Dockerfile              # Node.js app container
│   ├── docker-compose.yml      # Multi-service orchestration
│   └── .env                    # Environment variables
├── 🗄️ Database
│   └── init.sql                # PostgreSQL schema & indexes
├── 🖥️ Application
│   ├── server.js               # Express read + write servers
│   ├── package.json            # Node.js dependencies
│   └── public/                 # Static web assets
│       ├── heatmap.html        # Latency heatmap page
│       └── latency.html        # Latency time-series page
├── 🔧 Scripts
│   ├── data/                   # Data generation tools
│   └── utils/                  # Database utilities
└── 📚 Documentation
    └── README.md               # This file
```

## 🏃‍♂️ Quick Start

### Prerequisites

- Docker & Docker Compose
- Node.js 18+ (for local development)

### 1. Clone & Setup

```bash
git clone <repository-url>
cd LatencyDashboard

# Create your local .env (see "Environment Variables" below for the full list).
cat > .env <<'EOF'
POSTGRES_DB=latency_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=change_me
DB_HOST=localhost
DB_PORT=5432
APP_PORT=3000
WRITE_PORT=3001
EOF
```

### 2. Start Services

```bash
# Start PostgreSQL + Application
docker compose up -d

# Check services are running
docker compose ps
```

### 3. Generate Test Data

```bash
# Generate 6 hours of trading data
cd scripts/data/
node generate_data.js 6 "$(date -u +%Y-%m-%d)T08:00:00Z"

# Check data was inserted
cd ../utils/
./db_status.sh
```

### 4. Access Dashboard

- **Main Dashboard**: http://localhost:3000
- **Latency Heatmap**: http://localhost:3000/latency-heatmap
- **Time Series**:    http://localhost:3000/latency
- **Read API**:       http://localhost:3000/api/latency
- **Write API**:      http://localhost:3001/api/latency

## 📊 API Reference

The application runs two HTTP servers:

| Server | Default Port | Purpose                |
| ------ | ------------ | ---------------------- |
| Read   | `APP_PORT`   | Dashboard pages + GETs |
| Write  | `WRITE_PORT` | Ingestion (POST)       |

### GET `/api/latency`

Today's 08:00–14:00 (Asia/Taipei) trading window, used by the heatmap.

```json
[
  { "timestamp": 1726250400, "broker": "BrokerA", "latency_ms": 25.334 }
]
```

### GET `/api/latency/timeseries`

Last 1 hour, used by the time-series page.

### POST `/api/latency`

Insert a single latency record. Request body:

```json
{
  "broker":     "BrokerA",        // required, ≤50 chars
  "latency_ms": 25.334,           // required, finite ≥ 0
  "timestamp":  "2026-05-05T08:00:00+08:00",  // optional, defaults to now
  "symbol":     "AAPL",           // optional, ≤20 chars
  "side":       "B",              // optional, "B" or "S"
  "price":      145.67,           // optional, finite ≥ 0
  "volume":     1000              // optional, integer ≥ 0
}
```

Returns `201` on success, `400` on validation errors, `500` on DB errors.

## 🔧 Configuration

### Environment Variables

Configure via `.env` file:

```env
# Database Configuration
POSTGRES_DB=latency_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=your_secure_password
DB_HOST=localhost
DB_PORT=5432

# Application Configuration
APP_PORT=3000
WRITE_PORT=3001
```

### Database Schema

```sql
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
```

### Timezone

The database, the server connection pool, and the dashboard pages all operate in
**`Asia/Taipei` (UTC+8)**. `init.sql` sets the database default timezone, and
`server.js` issues `SET TIME ZONE 'Asia/Taipei'` on every checked-out connection,
so `CURRENT_DATE`, `NOW()`, and `EXTRACT(...)` are consistent across every layer.

## 🎯 Usage Examples

### Generate Today's Trading Data
```bash
cd scripts/data/
node generate_data.js 6 "$(date -u +%Y-%m-%d)T08:00:00Z"
```

### Clear All Data
```bash
cd scripts/utils/
./clear_data.sh
```

### Load Historical Data
```bash
cd scripts/utils/
./load_data.sh path/to/historical_data.sql
```

### Check Database Status
```bash
cd scripts/utils/
./db_status.sh
```

## 📈 Data Model

### Latency Data Structure

- **timestamp**: Order execution timestamp (microsecond precision)
- **broker**: Broker identifier (BrokerA, BrokerB, BrokerC)
- **latency_ms**: Order execution latency in milliseconds
- **symbol**: Stock symbol (AAPL, MSFT, NVDA, etc.)
- **side**: Order side (B=Buy, S=Sell)
- **price**: Order price
- **volume**: Order volume

### Data Generation

- **Frequency**: Every 5 seconds during trading hours
- **Distribution**: Normal distribution (μ=30ms, σ=5ms, min=1ms)
- **Time Range**: Configurable (default: 6-hour trading session)
- **Symbols**: Major tech stocks (AAPL, MSFT, NVDA, TSLA, etc.)

## 🔍 Visualization Features

### Heatmap Controls

- **Broker Filter**: View specific broker or all brokers
- **Time Range**: Fixed 08:00-14:00 trading session
- **Auto Y-axis**: Dynamic latency range based on data
- **Color Coding**: HSL gradient (cyan → magenta) for density

### Statistics Panel

- **Total Samples**: Number of data points
- **Average Latency**: Mean execution time
- **Maximum Latency**: Worst-case execution time
- **99th Percentile**: P99 latency for SLA monitoring

## 🚀 Development

### Local Development

```bash
# Install dependencies
npm install

# Start development server
npm run dev

# Start database only
docker compose up postgres -d
```

### Database Management

```bash
# Connect to database
docker compose exec postgres psql -U admin -d latency_db

# View logs
docker compose logs -f app
docker compose logs -f postgres
```

## 🐛 Troubleshooting

### Common Issues

**Database Connection Failed**
```bash
# Check if PostgreSQL is running
docker compose ps postgres

# Check logs
docker compose logs postgres

# Verify environment variables
cat .env
```

**No Data Visible**
```bash
# Check if data exists
cd scripts/utils && ./db_status.sh

# Generate test data
cd ../data && node generate_data.js 6
```

**Port Already in Use**
```bash
# Change APP_PORT in .env
echo "APP_PORT=3001" >> .env

# Restart services
docker compose down && docker compose up -d
```

## 📋 TODO / Roadmap

- [ ] Real-time streaming data integration
- [ ] Historical data retention policies
- [ ] Additional broker connections
- [ ] Alert system for latency thresholds
- [ ] Export functionality (CSV, JSON)
- [ ] Multi-timezone support
- [ ] Performance benchmarking dashboard

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📞 Support

For questions or issues:
- Create an issue in this repository
- Check the troubleshooting section above
- Review the scripts documentation in `scripts/README.md`