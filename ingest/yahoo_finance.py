"""
Yahoo Finance data fetcher using yfinance.
Returns normalised time-series points ready for the repository layer.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import yfinance as yf


SOURCE_NAME = "Yahoo Finance"
SOURCE_ENDPOINT = "https://query1.finance.yahoo.com"
SOURCE_DESCRIPTION = "Yahoo Finance public market data feed via yfinance library."


def fetch_ticker_info(symbol: str) -> dict[str, Any]:
    """Return metadata for a ticker (name, sector, currency, etc.)."""
    ticker = yf.Ticker(symbol)
    info = ticker.info or {}
    return {
        "symbol": symbol.upper(),
        "name": info.get("longName") or info.get("shortName") or symbol,
        "asset_class": _infer_class(info),
        "description": info.get("longBusinessSummary", ""),
        "region": _infer_region(info),
        "currency": info.get("currency", "USD"),
        "extra_attributes": {
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "exchange": info.get("exchange"),
            "market_cap": info.get("marketCap"),
        },
    }


def fetch_history(
    symbol: str,
    period: str = "1y",
    interval: str = "1d",
    start: str | None = None,
    end: str | None = None,
) -> list[dict[str, Any]]:
    """
    Download OHLCV history for `symbol`.
    Returns a list of point dicts (without series_id / ingestion_id — caller fills those).
    """
    ticker = yf.Ticker(symbol)
    kwargs: dict[str, Any] = {"interval": interval, "auto_adjust": True}
    if start and end:
        kwargs["start"] = start
        kwargs["end"] = end
    else:
        kwargs["period"] = period

    df = ticker.history(**kwargs)
    if df.empty:
        return []

    points = []
    for ts, row in df.iterrows():
        # yfinance index is tz-aware; normalise to UTC
        if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
            dt = ts.to_pydatetime().astimezone(timezone.utc).replace(tzinfo=None)
        else:
            dt = ts.to_pydatetime().replace(tzinfo=None)

        points.append({
            "timestamp": dt,
            "open": float(row.get("Open", 0) or 0),
            "high": float(row.get("High", 0) or 0),
            "low": float(row.get("Low", 0) or 0),
            "close": float(row.get("Close", 0) or 0),
            "volume": int(row.get("Volume", 0) or 0),
            "extra_attributes": {
                "dividends": float(row.get("Dividends", 0) or 0),
                "stock_splits": float(row.get("Stock Splits", 0) or 0),
            },
        })
    return points


# ── helpers ───────────────────────────────────────────────────────────────────

_CRYPTO_SUFFIXES = ("-USD", "-EUR", "-BTC", "-ETH")
_ETF_TYPES = {"ETF", "MUTUALFUND"}
_BOND_TYPES = {"BOND", "FIXED INCOME"}


def _infer_class(info: dict) -> str:
    q_type = (info.get("quoteType") or "").upper()
    if q_type == "CRYPTOCURRENCY":
        return "crypto"
    if q_type in _ETF_TYPES:
        return "etf"
    if q_type == "FUTURE":
        return "futures"
    if q_type == "INDEX":
        return "index"
    if q_type in _BOND_TYPES:
        return "bond"
    return "stock"


def _infer_region(info: dict) -> str:
    exchange = (info.get("exchange") or "").upper()
    country = info.get("country") or ""
    if exchange in ("NYQ", "NMS", "NGM", "PCX", "BTS"):
        return "US"
    if exchange in ("LSE",):
        return "Europe"
    if exchange in ("TSX",):
        return "Canada"
    if country:
        return country
    return "Unknown"
