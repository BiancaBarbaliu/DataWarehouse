# Acme Financial Data Warehouse

A temporal data warehouse for financial market data built with Python, FastAPI, and MongoDB.

[Watch Demo] https://drive.google.com/drive/u/0/folders/1neFWSytJQAo4GAoYHNGNgOtvIqI_1Tlj

## Architecture

```
acme-financial-dw/
├── db/                  # MongoDB client + temporal repository
├── ingest/              # Yahoo Finance fetcher + pipeline
├── api/                 # FastAPI REST API (UC2: Q1–Q5)
│   └── routers/         # assets, sources, timeseries
├── analytics/           # Aggregations, trends, forecasts (UC3)
├── mcp_server/          # MCP server for Claude integration (UC4)
├── docker-compose.yml
└── requirements.txt
```

**Storage:** MongoDB (NoSQL, document store, flexible schema for heterogeneous assets)  
**Temporal model:** Append-only. Records are never updated or deleted in-place. Changes create new `AssetVersion` documents. "Deletion" is a marker version with `is_deleted=True`.

---

## Quick Start

### Option A — Docker (recommended)

```bash
# 1. Start MongoDB + API + MCP server
docker-compose up --build

# API: http://localhost:8000
# Docs: http://localhost:8000/docs
```

### Option B — Local (no Docker)

```bash
# 1. Start MongoDB locally (or use MongoDB Atlas)
# 2. Install dependencies
pip install -r requirements.txt

# 3. Set env vars
cp .env.example .env
# Edit .env with your MONGO_URI if needed

# 4. Run API
uvicorn api.main:app --reload

# 5. (Optional) Run MCP server separately
python -m mcp_server.server
```

---

## Ingest Data

```bash
# Ingest 1 year of daily OHLCV data for several assets
python -m ingest.pipeline --symbols AAPL MSFT GOOGL BTC-USD GC=F --period 1y

# Or via the API (POST)
curl -X POST http://localhost:8000/assets/ingest \
  -H "Content-Type: application/json" \
  -d '{"symbols": ["AAPL", "TSLA", "BTC-USD"], "period": "1y"}'
```

Supported asset types via Yahoo Finance:
- Stocks: `AAPL`, `MSFT`, `TSLA`, `AMZN`, `GOOGL`
- Crypto: `BTC-USD`, `ETH-USD`
- Commodities/Futures: `GC=F` (Gold), `CL=F` (Oil)
- Indices: `^GSPC` (S&P 500), `^DJI` (Dow Jones)

---

## REST API Reference

Full interactive docs at: **http://localhost:8000/docs**

| Query | Method | Endpoint |
|-------|--------|----------|
| Q1 – List assets | GET | `/assets` |
| Q2 – Asset detail | GET | `/assets/{asset_id}` |
| Q3 – List sources | GET | `/sources` |
| Q4 – Source detail | GET | `/sources/{source_id}` |
| Q5 – Time series | GET | `/timeseries/{asset_id}/{source_id}` |
| Analytics – Stats | GET | `/analytics/stats/{asset_id}/{source_id}` |
| Analytics – Trend | GET | `/analytics/trend/{asset_id}/{source_id}` |
| Analytics – Forecast | GET | `/analytics/forecast/{asset_id}/{source_id}` |
| Analytics – Risk | GET | `/analytics/risk/{asset_id}/{source_id}` |
| Analytics – Compare | POST | `/analytics/compare` |
| CSV Export (Spark) | GET | `/timeseries/export/{series_id}` |

### Example API calls

```bash
# List all assets
curl http://localhost:8000/assets

# Get asset details
curl http://localhost:8000/assets/<asset_id>

# Get time series (last 30 days)
curl "http://localhost:8000/timeseries/<asset_id>/<source_id>?limit=30"

# Get statistics
curl http://localhost:8000/analytics/stats/<asset_id>/<source_id>

# Forecast next day
curl http://localhost:8000/analytics/forecast/<asset_id>/<source_id>

# Export CSV for Spark
curl http://localhost:8000/timeseries/export/<series_id> -o data.csv
```

---

## MCP Server (Claude Integration — UC4)

The MCP server lets Claude explore the warehouse using natural language.

### Connect Claude Desktop to the MCP server

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "acme-financial-dw": {
      "command": "python",
      "args": ["-m", "mcp_server.server"],
      "cwd": "/path/to/acme-financial-dw",
      "env": {
        "MONGO_URI": "mongodb://localhost:27017",
        "MONGO_DB": "acme_dw"
      }
    }
  }
}
```

### Available MCP tools

| Tool | Description |
|------|-------------|
| `list_assets` | List all financial assets |
| `get_asset` | Get asset by symbol or id |
| `list_sources` | List all data sources |
| `fetch_time_series` | Get OHLCV data for asset+source |
| `get_stats` | Min/max/avg/std for a series |
| `get_trend` | Price trend + moving average |
| `forecast_next_day` | Next-day close price prediction |
| `get_risk` | Volatility + risk score |
| `compare_assets` | Normalised return comparison |
| `ingest_asset` | Trigger ingestion for a symbol |

### Example prompts for Claude

- *"List all assets in the warehouse"*
- *"What is the risk level of Apple stock?"*
- *"Compare AAPL and MSFT performance over the past year"*
- *"Ingest Tesla data and then forecast tomorrow's closing price"*
- *"What are the stats for Bitcoin over the last year?"*
- *"Find all assets → fetch time series → compute summaries → explain trends"* (agentic)

---

## Apache Spark Integration (UC3)

Export any time series as CSV and load into Spark:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AcmeDW").getOrCreate()

# Load exported CSV
df = spark.read.option("header", "true").csv("series_<id>.csv")
df = df.withColumn("close", df["close"].cast("double"))
df.createOrReplaceTempView("prices")

# Example aggregation
spark.sql("SELECT MIN(close), MAX(close), AVG(close) FROM prices").show()
```

---

## Temporal Data Warehouse

The system implements the temporal DWH paradigm:

- **No updates/deletes in-place** — MongoDB documents are immutable once written
- **Asset changes** create a new `AssetVersion` document with `valid_from` timestamp
- **"Deletion"** inserts a version with `is_deleted=True` and a `valid_from` timestamp
- **Historical queries** use `get_asset_at(asset_id, at=datetime(...))` to retrieve the state at any past point
- **Time-series points** are upserted with `$setOnInsert` — duplicate timestamps are silently ignored

---

## MongoDB Collections

| Collection | Purpose |
|------------|---------|
| `financial_assets` | Core asset metadata |
| `asset_versions` | Temporal history of asset changes |
| `data_sources` | Vendor/provider registry |
| `time_series` | Asset+source combinations |
| `time_series_points` | OHLCV data points (append-only) |
| `ingestion_events` | Provenance log of every ingest run |
