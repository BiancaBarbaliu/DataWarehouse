"""
Ingest pipeline: fetch from Yahoo Finance → store in MongoDB.

Usage (CLI):
    python -m ingest.pipeline --symbols AAPL MSFT BTC-USD --period 1y
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime

from db import client as db_client
from db import repository as repo
from ingest.yahoo_finance import (
    SOURCE_DESCRIPTION, SOURCE_ENDPOINT, SOURCE_NAME,
    fetch_history, fetch_ticker_info,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def ingest_symbol(
    symbol: str,
    period: str = "1y",
    interval: str = "1d",
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """
    Full ingest for one symbol:
      1. Ensure DataSource exists.
      2. Fetch ticker metadata → upsert FinancialAsset.
      3. Create IngestionEvent.
      4. Fetch OHLCV history.
      5. Upsert TimeSeries.
      6. Append TimeSeriesPoints.
      7. Mark IngestionEvent as complete.
    Returns a summary dict.
    """
    # 1. data source
    source = repo.upsert_source(SOURCE_NAME, SOURCE_ENDPOINT, SOURCE_DESCRIPTION)
    source_id = source["_id"]

    # 2. asset metadata
    logger.info("Fetching metadata for %s", symbol)
    info = fetch_ticker_info(symbol)

    existing = repo.get_asset_by_symbol(symbol)
    if existing:
        asset_id = existing["_id"]
        logger.info("Asset %s already exists (id=%s)", symbol, asset_id)
    else:
        asset = repo.create_asset(
            symbol=info["symbol"],
            asset_class=info["asset_class"],
            description=info["description"][:500] if info["description"] else info["name"],
            region=info["region"],
            extra_attributes=info["extra_attributes"],
        )
        asset_id = asset["_id"]
        logger.info("Created asset %s (id=%s)", symbol, asset_id)

    # 3. ingestion event
    request_params = {
        "symbol": symbol,
        "period": period,
        "interval": interval,
        "start": start,
        "end": end,
    }
    event = repo.create_ingestion_event(source_id, request_params)
    event_id = event["_id"]

    # 4. fetch history
    logger.info("Fetching history for %s (period=%s, interval=%s)", symbol, period, interval)
    try:
        raw_points = fetch_history(symbol, period=period, interval=interval, start=start, end=end)
    except Exception as exc:
        repo.finish_ingestion_event(event_id, 0, str(exc))
        logger.error("Failed to fetch history for %s: %s", symbol, exc)
        return {"symbol": symbol, "status": "failed", "error": str(exc)}

    # 5. time series
    ts = repo.upsert_time_series(asset_id, source_id, interval, info.get("currency", "USD"))
    series_id = ts["_id"]

    # 6. build point docs
    points = [
        {
            "series_id": series_id,
            "ingestion_id": event_id,
            "timestamp": p["timestamp"],
            "open": p["open"],
            "high": p["high"],
            "low": p["low"],
            "close": p["close"],
            "volume": p["volume"],
            "extra_attributes": p.get("extra_attributes", {}),
        }
        for p in raw_points
    ]
    inserted = repo.insert_ts_points(points)
    logger.info("Inserted %d new points for %s", inserted, symbol)

    # 7. finish event
    repo.finish_ingestion_event(event_id, inserted)

    return {
        "symbol": symbol,
        "asset_id": asset_id,
        "series_id": series_id,
        "source_id": source_id,
        "ingestion_id": event_id,
        "total_fetched": len(raw_points),
        "new_points": inserted,
        "status": "completed",
    }


def ingest_many(symbols: list[str], period: str = "1y", interval: str = "1d") -> list[dict]:
    db_client.ensure_indexes()
    results = []
    for sym in symbols:
        result = ingest_symbol(sym, period=period, interval=interval)
        results.append(result)
    return results


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest financial data from Yahoo Finance")
    parser.add_argument("--symbols", nargs="+", required=True, help="Ticker symbols, e.g. AAPL MSFT BTC-USD")
    parser.add_argument("--period", default="1y", help="yfinance period (default: 1y)")
    parser.add_argument("--interval", default="1d", help="yfinance interval (default: 1d)")
    args = parser.parse_args()

    summaries = ingest_many(args.symbols, period=args.period, interval=args.interval)
    for s in summaries:
        print(s)
