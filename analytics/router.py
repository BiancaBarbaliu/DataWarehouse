"""Analytics API endpoints (UC3) — NumPy + Apache Spark."""
from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from analytics import engine
from db import repository as repo

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def _resolve_series(asset_id: str, source_id: str) -> str:
    ts = repo.get_time_series_for_asset_source(asset_id, source_id)
    if not ts:
        raise HTTPException(404, "No time series found for this asset+source combination")
    return ts["_id"]


@router.get("/stats/{asset_id}/{source_id}", summary="Aggregated statistics (min/max/avg/std)")
def stats(
    asset_id: str,
    source_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
):
    series_id = _resolve_series(asset_id, source_id)
    from_dt = datetime.fromisoformat(from_date) if from_date else None
    to_dt = datetime.fromisoformat(to_date) if to_date else None
    return engine.compute_stats(series_id, from_dt=from_dt, to_dt=to_dt)


@router.get("/trend/{asset_id}/{source_id}", summary="Price trend + moving average")
def trend(
    asset_id: str,
    source_id: str,
    window: Annotated[int, Query(ge=2, le=200)] = 20,
):
    series_id = _resolve_series(asset_id, source_id)
    return engine.compute_trend(series_id, window=window)


@router.get("/forecast/{asset_id}/{source_id}", summary="Next-day price forecast (NumPy)")
def forecast(
    asset_id: str,
    source_id: str,
    lookback: Annotated[int, Query(ge=5, le=365)] = 30,
):
    series_id = _resolve_series(asset_id, source_id)
    return engine.forecast_next_day(series_id, lookback=lookback)


@router.get("/risk/{asset_id}/{source_id}", summary="Volatility and risk score")
def risk(asset_id: str, source_id: str):
    series_id = _resolve_series(asset_id, source_id)
    return engine.compute_risk(series_id)


@router.post("/compare", summary="Compare multiple assets by normalised return")
def compare(body: dict):
    """
    Body: {"pairs": [{"asset_id": "...", "source_id": "..."}, ...]}
    """
    pairs = body.get("pairs", [])
    if not pairs:
        raise HTTPException(400, "Provide at least one pair")
    series_ids = []
    for p in pairs:
        ts = repo.get_time_series_for_asset_source(p["asset_id"], p["source_id"])
        if ts:
            series_ids.append(ts["_id"])
    return engine.compare_assets(series_ids)


# ── Apache Spark endpoints ────────────────────────────────────────────────────

@router.get(
    "/spark/stats/{asset_id}/{source_id}",
    summary="Spark aggregations (min/max/avg/stddev via Apache Spark DataFrame)",
)
def spark_stats(asset_id: str, source_id: str):
    """
    Runs a real Apache Spark job to compute aggregations on the time series.
    Requires PySpark to be installed: pip install pyspark
    """
    series_id = _resolve_series(asset_id, source_id)
    try:
        from analytics.spark_jobs import spark_aggregations
        return spark_aggregations(series_id)
    except RuntimeError as e:
        raise HTTPException(503, str(e))


@router.get(
    "/spark/predict/{asset_id}/{source_id}",
    summary="Spark MLlib linear regression — next-day close price prediction",
)
def spark_predict(
    asset_id: str,
    source_id: str,
    lookback: Annotated[int, Query(ge=10, le=365)] = 60,
):
    """
    Trains a Spark MLlib LinearRegression model on historical close prices
    and predicts the next day's closing price.
    Requires PySpark: pip install pyspark
    """
    series_id = _resolve_series(asset_id, source_id)
    try:
        from analytics.spark_jobs import spark_predict_next_day
        return spark_predict_next_day(series_id, lookback=lookback)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
