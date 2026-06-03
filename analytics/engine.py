"""
Analytics engine: aggregations, trend analysis, and simple forecasting.
All computations run in Python/NumPy — no Spark dependency required locally.
Data can be exported as CSV for feeding into a Spark job.
"""
from __future__ import annotations

import statistics
from datetime import datetime, timedelta
from typing import Any

import numpy as np

from db import repository as repo


# ── Aggregations ──────────────────────────────────────────────────────────────

def compute_stats(series_id: str, from_dt: datetime | None = None, to_dt: datetime | None = None) -> dict:
    """Return count / min / max / avg / std for close prices of a time series."""
    points = repo.get_ts_points(series_id, from_dt=from_dt, to_dt=to_dt, limit=100_000)
    if not points:
        return {"error": "No data found"}

    closes = [p["close"] for p in points if p["close"] is not None]
    volumes = [p["volume"] for p in points if p["volume"] is not None]

    return {
        "series_id": series_id,
        "from": str(points[0]["timestamp"]),
        "to": str(points[-1]["timestamp"]),
        "count": len(closes),
        "close": {
            "min": round(min(closes), 4),
            "max": round(max(closes), 4),
            "avg": round(statistics.mean(closes), 4),
            "std": round(statistics.stdev(closes) if len(closes) > 1 else 0.0, 4),
        },
        "volume": {
            "min": min(volumes),
            "max": max(volumes),
            "avg": round(statistics.mean(volumes), 2),
        },
    }


# ── Trend ─────────────────────────────────────────────────────────────────────

def compute_trend(series_id: str, window: int = 20) -> dict:
    """
    Compute a simple linear regression trend on close prices.
    Returns slope (per day), direction, and a moving-average series.
    """
    points = repo.get_ts_points(series_id, limit=100_000)
    if len(points) < 2:
        return {"error": "Not enough data"}

    closes = np.array([p["close"] for p in points], dtype=float)
    timestamps = [p["timestamp"] for p in points]

    # x = days from first point
    x = np.arange(len(closes), dtype=float)
    slope, intercept = np.polyfit(x, closes, 1)

    # moving average
    ma = []
    for i in range(len(closes)):
        start = max(0, i - window + 1)
        ma.append(round(float(np.mean(closes[start:i+1])), 4))

    return {
        "series_id": series_id,
        "points": len(closes),
        "slope_per_day": round(float(slope), 6),
        "direction": "up" if slope > 0 else "down",
        "moving_average": [
            {"timestamp": str(timestamps[i]), "ma": ma[i]}
            for i in range(len(ma))
        ],
    }


# ── Simple next-day forecast (linear extrapolation) ───────────────────────────

def forecast_next_day(series_id: str, lookback: int = 30) -> dict:
    """
    Predict next day's close price using linear regression on the last `lookback` points.
    """
    points = repo.get_ts_points(series_id, limit=100_000)
    if len(points) < 2:
        return {"error": "Not enough data"}

    recent = points[-lookback:]
    closes = np.array([p["close"] for p in recent], dtype=float)
    x = np.arange(len(closes), dtype=float)

    slope, intercept = np.polyfit(x, closes, 1)
    predicted = slope * len(closes) + intercept
    last_close = closes[-1]
    last_ts: datetime = recent[-1]["timestamp"]
    next_ts = last_ts + timedelta(days=1)

    return {
        "series_id": series_id,
        "last_close": round(float(last_close), 4),
        "last_date": str(last_ts),
        "predicted_close": round(float(predicted), 4),
        "predicted_date": str(next_ts.date()),
        "change_pct": round((predicted - last_close) / last_close * 100, 3),
        "method": f"linear_regression_lookback_{lookback}",
    }


# ── Comparison ────────────────────────────────────────────────────────────────

def compare_assets(series_ids: list[str]) -> dict:
    """
    Compare multiple time series by their normalised return (% change from first point).
    Useful for side-by-side chart data.
    """
    result: dict[str, Any] = {}
    for sid in series_ids:
        points = repo.get_ts_points(sid, limit=100_000)
        if not points:
            result[sid] = {"error": "no data"}
            continue
        base = points[0]["close"]
        series = [
            {
                "timestamp": str(p["timestamp"]),
                "close": p["close"],
                "return_pct": round((p["close"] - base) / base * 100, 3) if base else 0,
            }
            for p in points
        ]
        result[sid] = {
            "count": len(series),
            "base_close": base,
            "latest_close": points[-1]["close"],
            "total_return_pct": series[-1]["return_pct"] if series else 0,
            "series": series,
        }
    return result


# ── Risk signal ───────────────────────────────────────────────────────────────

def compute_risk(series_id: str, window: int = 20) -> dict:
    """
    Compute daily returns, volatility (std of returns), and a simple risk score.
    """
    points = repo.get_ts_points(series_id, limit=100_000)
    if len(points) < 2:
        return {"error": "Not enough data"}

    closes = np.array([p["close"] for p in points], dtype=float)
    daily_returns = np.diff(closes) / closes[:-1]

    volatility = float(np.std(daily_returns))
    annualised_vol = volatility * np.sqrt(252)

    # simple risk bucket
    if annualised_vol < 0.15:
        risk_level = "low"
    elif annualised_vol < 0.35:
        risk_level = "medium"
    else:
        risk_level = "high"

    return {
        "series_id": series_id,
        "daily_volatility": round(volatility, 6),
        "annualised_volatility": round(annualised_vol, 4),
        "risk_level": risk_level,
        "avg_daily_return": round(float(np.mean(daily_returns)), 6),
        "sharpe_approx": round(float(np.mean(daily_returns) / (volatility or 1e-9)) * np.sqrt(252), 4),
    }
