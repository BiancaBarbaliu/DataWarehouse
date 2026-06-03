"""
Time-series endpoints:
  GET /timeseries                          → list all series (asset+source pairs)
  GET /timeseries/{asset_id}/{source_id}   → Q5: points for asset+source
  GET /timeseries/export/{series_id}       → CSV export for Spark/analytics
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db import repository as repo
from db.client import col_time_series

router = APIRouter(prefix="/timeseries", tags=["Time Series"])


class TSPoint(BaseModel):
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    extra_attributes: dict = {}


class TSResponse(BaseModel):
    series_id: str
    asset_id: str
    source_id: str
    frequency: str
    currency: str
    count: int
    points: list[TSPoint]


@router.get(
    "/{asset_id}/{source_id}",
    response_model=TSResponse,
    summary="Q5 – Time-series data for asset + source",
)
def get_time_series(
    asset_id: str,
    source_id: str,
    from_date: Annotated[str | None, Query(description="ISO date, e.g. 2023-01-01")] = None,
    to_date: Annotated[str | None, Query(description="ISO date, e.g. 2024-01-01")] = None,
    limit: Annotated[int, Query(ge=1, le=5000)] = 500,
):
    """Return time-series data points for a given asset and data source."""
    ts = repo.get_time_series_for_asset_source(asset_id, source_id)
    if not ts:
        raise HTTPException(404, "No time series found for this asset+source combination")

    from_dt = datetime.fromisoformat(from_date) if from_date else None
    to_dt = datetime.fromisoformat(to_date) if to_date else None

    raw = repo.get_ts_points(ts["_id"], from_dt=from_dt, to_dt=to_dt, limit=limit)

    points = [
        TSPoint(
            timestamp=str(p["timestamp"]),
            open=p["open"],
            high=p["high"],
            low=p["low"],
            close=p["close"],
            volume=p["volume"],
            extra_attributes=p.get("extra_attributes", {}),
        )
        for p in raw
    ]

    return TSResponse(
        series_id=ts["_id"],
        asset_id=asset_id,
        source_id=source_id,
        frequency=ts.get("frequency", "1d"),
        currency=ts.get("currency", "USD"),
        count=len(points),
        points=points,
    )


@router.get("/export/{series_id}", summary="Export time series as CSV (Spark-ready)")
def export_csv(
    series_id: str,
    from_date: str | None = None,
    to_date: str | None = None,
):
    """Stream a CSV file of the time series — suitable for loading into Apache Spark."""
    ts = repo.get_time_series(series_id)
    if not ts:
        raise HTTPException(404, f"Series {series_id!r} not found")

    from_dt = datetime.fromisoformat(from_date) if from_date else None
    to_dt = datetime.fromisoformat(to_date) if to_date else None
    points = repo.get_ts_points(series_id, from_dt=from_dt, to_dt=to_dt, limit=100_000)

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=["timestamp", "open", "high", "low", "close", "volume"],
        extrasaction="ignore",
    )
    writer.writeheader()
    for p in points:
        writer.writerow({
            "timestamp": p["timestamp"].isoformat() if hasattr(p["timestamp"], "isoformat") else p["timestamp"],
            "open": p["open"],
            "high": p["high"],
            "low": p["low"],
            "close": p["close"],
            "volume": p["volume"],
        })

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=series_{series_id}.csv"},
    )
