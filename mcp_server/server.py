"""
MCP server for Acme Financial Data Warehouse.

Exposes the platform's capabilities as tools so Claude (or any MCP-compatible
LLM) can explore and reason over financial data in natural language.

Tools exposed:
  list_assets          – list all financial assets in the warehouse
  get_asset            – get full details of one asset by symbol or id
  list_sources         – list all data sources
  fetch_time_series    – get OHLCV data for an asset + source
  get_stats            – aggregated stats (min/max/avg/std) for a series
  get_trend            – price trend and moving average
  forecast_next_day    – predict next close price
  get_risk             – volatility and risk score
  compare_assets       – compare multiple assets by normalised return
  ingest_asset         – trigger ingestion for a ticker symbol

Run:
  python -m mcp_server.server
"""
from __future__ import annotations

import json
import os
import sys
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server

# Add project root to path when running as module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics import engine as analytics
from db import repository as repo
from db.client import ensure_indexes
from ingest.pipeline import ingest_symbol

# ── server setup ──────────────────────────────────────────────────────────────

server = Server("acme-financial-dw")


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_assets",
            description="List all financial assets in the data warehouse. Returns id, symbol, asset class and region.",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "default": 50, "description": "Max number of assets to return"},
                },
            },
        ),
        types.Tool(
            name="get_asset",
            description="Get full details of a financial asset by its symbol (e.g. AAPL, BTC-USD) or asset id.",
            inputSchema={
                "type": "object",
                "properties": {
                    "symbol": {"type": "string", "description": "Ticker symbol, e.g. AAPL"},
                    "asset_id": {"type": "string", "description": "MongoDB asset id (alternative to symbol)"},
                },
            },
        ),
        types.Tool(
            name="list_sources",
            description="List all financial data sources (vendors) registered in the warehouse.",
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="fetch_time_series",
            description=(
                "Fetch OHLCV (open/high/low/close/volume) time series data for a given asset and data source. "
                "Use list_assets to get asset_id and list_sources to get source_id."
            ),
            inputSchema={
                "type": "object",
                "required": ["asset_id", "source_id"],
                "properties": {
                    "asset_id": {"type": "string"},
                    "source_id": {"type": "string"},
                    "from_date": {"type": "string", "description": "ISO date e.g. 2023-01-01"},
                    "to_date": {"type": "string", "description": "ISO date e.g. 2024-01-01"},
                    "limit": {"type": "integer", "default": 30},
                },
            },
        ),
        types.Tool(
            name="get_stats",
            description="Get aggregated statistics (count, min, max, avg, std) for close prices of an asset.",
            inputSchema={
                "type": "object",
                "required": ["asset_id", "source_id"],
                "properties": {
                    "asset_id": {"type": "string"},
                    "source_id": {"type": "string"},
                    "from_date": {"type": "string"},
                    "to_date": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="get_trend",
            description="Compute price trend (slope direction + moving average) for an asset.",
            inputSchema={
                "type": "object",
                "required": ["asset_id", "source_id"],
                "properties": {
                    "asset_id": {"type": "string"},
                    "source_id": {"type": "string"},
                    "window": {"type": "integer", "default": 20, "description": "Moving average window in days"},
                },
            },
        ),
        types.Tool(
            name="forecast_next_day",
            description="Predict the next day's closing price for an asset using linear regression.",
            inputSchema={
                "type": "object",
                "required": ["asset_id", "source_id"],
                "properties": {
                    "asset_id": {"type": "string"},
                    "source_id": {"type": "string"},
                    "lookback": {"type": "integer", "default": 30},
                },
            },
        ),
        types.Tool(
            name="get_risk",
            description="Compute volatility and risk score (low/medium/high) for an asset.",
            inputSchema={
                "type": "object",
                "required": ["asset_id", "source_id"],
                "properties": {
                    "asset_id": {"type": "string"},
                    "source_id": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="compare_assets",
            description="Compare multiple assets by their normalised return (% change from first data point).",
            inputSchema={
                "type": "object",
                "required": ["pairs"],
                "properties": {
                    "pairs": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["asset_id", "source_id"],
                            "properties": {
                                "asset_id": {"type": "string"},
                                "source_id": {"type": "string"},
                            },
                        },
                        "description": "List of {asset_id, source_id} pairs to compare",
                    }
                },
            },
        ),
        types.Tool(
            name="ingest_asset",
            description=(
                "Ingest financial data for a ticker symbol from Yahoo Finance. "
                "This fetches and stores historical OHLCV data. Use this before querying an asset that doesn't exist yet."
            ),
            inputSchema={
                "type": "object",
                "required": ["symbol"],
                "properties": {
                    "symbol": {"type": "string", "description": "Ticker e.g. AAPL, MSFT, BTC-USD, GC=F"},
                    "period": {"type": "string", "default": "1y", "description": "yfinance period: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max"},
                    "interval": {"type": "string", "default": "1d", "description": "yfinance interval: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo"},
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
    ensure_indexes()

    try:
        result = await _dispatch(name, arguments)
    except Exception as exc:
        result = {"error": str(exc)}

    return [types.TextContent(type="text", text=json.dumps(result, default=str, indent=2))]


async def _dispatch(name: str, args: dict) -> Any:
    # ── list_assets ────────────────────────────────────────────────────────────
    if name == "list_assets":
        limit = int(args.get("limit", 50))
        assets = repo.list_assets(limit=limit)
        return {"count": len(assets), "assets": assets}

    # ── get_asset ──────────────────────────────────────────────────────────────
    elif name == "get_asset":
        symbol = args.get("symbol")
        asset_id = args.get("asset_id")
        if symbol:
            doc = repo.get_asset_by_symbol(symbol)
        elif asset_id:
            doc = repo.get_asset_by_id(asset_id)
        else:
            return {"error": "Provide either symbol or asset_id"}
        if not doc:
            return {"error": f"Asset not found"}
        return doc

    # ── list_sources ───────────────────────────────────────────────────────────
    elif name == "list_sources":
        return {"sources": repo.list_sources()}

    # ── fetch_time_series ──────────────────────────────────────────────────────
    elif name == "fetch_time_series":
        from datetime import datetime
        asset_id = args["asset_id"]
        source_id = args["source_id"]
        ts = repo.get_time_series_for_asset_source(asset_id, source_id)
        if not ts:
            return {"error": "No time series found for this asset+source combination"}
        from_dt = datetime.fromisoformat(args["from_date"]) if args.get("from_date") else None
        to_dt = datetime.fromisoformat(args["to_date"]) if args.get("to_date") else None
        limit = int(args.get("limit", 30))
        points = repo.get_ts_points(ts["_id"], from_dt=from_dt, to_dt=to_dt, limit=limit)
        return {
            "series_id": ts["_id"],
            "asset_id": asset_id,
            "source_id": source_id,
            "frequency": ts.get("frequency"),
            "currency": ts.get("currency"),
            "count": len(points),
            "points": points,
        }

    # ── get_stats ──────────────────────────────────────────────────────────────
    elif name == "get_stats":
        from datetime import datetime
        ts = repo.get_time_series_for_asset_source(args["asset_id"], args["source_id"])
        if not ts:
            return {"error": "No time series found"}
        from_dt = datetime.fromisoformat(args["from_date"]) if args.get("from_date") else None
        to_dt = datetime.fromisoformat(args["to_date"]) if args.get("to_date") else None
        return analytics.compute_stats(ts["_id"], from_dt=from_dt, to_dt=to_dt)

    # ── get_trend ──────────────────────────────────────────────────────────────
    elif name == "get_trend":
        ts = repo.get_time_series_for_asset_source(args["asset_id"], args["source_id"])
        if not ts:
            return {"error": "No time series found"}
        result = analytics.compute_trend(ts["_id"], window=int(args.get("window", 20)))
        # trim moving_average to last 10 points to keep response concise
        if "moving_average" in result:
            result["moving_average"] = result["moving_average"][-10:]
        return result

    # ── forecast_next_day ──────────────────────────────────────────────────────
    elif name == "forecast_next_day":
        ts = repo.get_time_series_for_asset_source(args["asset_id"], args["source_id"])
        if not ts:
            return {"error": "No time series found"}
        return analytics.forecast_next_day(ts["_id"], lookback=int(args.get("lookback", 30)))

    # ── get_risk ───────────────────────────────────────────────────────────────
    elif name == "get_risk":
        ts = repo.get_time_series_for_asset_source(args["asset_id"], args["source_id"])
        if not ts:
            return {"error": "No time series found"}
        return analytics.compute_risk(ts["_id"])

    # ── compare_assets ─────────────────────────────────────────────────────────
    elif name == "compare_assets":
        pairs = args.get("pairs", [])
        series_ids = []
        for p in pairs:
            ts = repo.get_time_series_for_asset_source(p["asset_id"], p["source_id"])
            if ts:
                series_ids.append(ts["_id"])
        result = analytics.compare_assets(series_ids)
        # slim down the series arrays to last 10 for readability
        for sid, data in result.items():
            if isinstance(data, dict) and "series" in data:
                data["series"] = data["series"][-10:]
        return result

    # ── ingest_asset ───────────────────────────────────────────────────────────
    elif name == "ingest_asset":
        symbol = args["symbol"].upper()
        period = args.get("period", "1y")
        interval = args.get("interval", "1d")
        return ingest_symbol(symbol, period=period, interval=interval)

    else:
        return {"error": f"Unknown tool: {name}"}


# ── entry point ───────────────────────────────────────────────────────────────

async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
