# Latency Dashboard Scripts

This folder contains organized scripts and utilities for the Latency Dashboard project.

## 📁 Folder Structure

```
scripts/
├── 📊 data/
│   └── generate_data.js          # Direct database data generator
├── 🔧 utils/
│   ├── clear_data.sh            # Data clearing tool
│   ├── load_data.sh             # SQL file loader
│   └── db_status.sh             # Database status checker
└── 📖 README.md                  # Complete usage guide
```

## 📊 Primary Data Generator (`data/`)

### generate_data.js ⭐
**Main data generator** - Directly inserts fake data into database using Node.js PostgreSQL client.

**Usage:**
```bash
cd data/
node generate_data.js [hours] [start_date]

# Examples:
node generate_data.js                           # 24 hours from 2025-09-14
node generate_data.js 6                         # 6 hours from 2025-09-14
node generate_data.js 6 "2025-09-15T08:00:00Z" # 6 hours from specific date
```

**Features:**
- ✅ Direct database insertion (no SQL files needed)
- ✅ Batch processing (1000 records per batch)
- ✅ Progress monitoring
- ✅ Database summary after insertion
- ✅ Environment variable support
- ✅ Flexible time ranges and start dates

## 🔧 Utilities (`utils/`)


### clear_data.sh
Safely clear all data from the order_latency table.

**Usage:**
```bash
cd utils/
./clear_data.sh  # Will ask for confirmation
```

### load_data.sh
Load SQL files into the database (for SQL-based workflows).

**Usage:**
```bash
cd utils/
./load_data.sh path/to/file.sql
```

### db_status.sh
Check database connectivity and show statistics.

**Usage:**
```bash
cd utils/
./db_status.sh
```

## 📋 Data Structure

All generated data includes:
- **timestamp**: Sequential timestamps (every 5 seconds)
- **broker**: Random selection from BrokerA, BrokerB, BrokerC
- **latency_ms**: Normal distribution (mean=30ms, stddev=5ms, min=1ms)
- **symbol**: Random stock symbols (AAPL, MSFT, NVDA, TSLA, AMZN, META, GOOGL, NFLX)
- **side**: Buy (B) or Sell (S)
- **price**: Random price between $100-600
- **volume**: Random volume between 100-600 shares

## 🚀 Quick Start

1. **Check database status:**
   ```bash
   cd utils/ && ./db_status.sh
   ```

2. **Generate trading day data (recommended):**
   ```bash
   cd data/
   node generate_data.js 6 "$(date -u +%Y-%m-%d)T08:00:00Z"
   ```

3. **Generate full day data:**
   ```bash
   cd data/
   node generate_data.js 24
   ```

4. **View the dashboard:**
   Open http://localhost:3000/latency-heatmap

## ⚙️ Environment

All scripts automatically load configuration from `../../.env`:
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `DB_HOST`, `DB_PORT`

## 📝 Notes

- **Recommended workflow**: Use `data/generate_data.js` for direct database insertion
- Main `init.sql` contains only table schema for clean initialization
- Dashboard focuses on 08:00-14:00 trading hours (UTC+8)
- Batch processing ensures good performance with large datasets
- All utilities include confirmation prompts for destructive operations