"""
Asset endpoints:
  GET  /assets          → Q1: list all assets (id + symbol + class + region)
  GET  /assets/{id}     → Q2: full details of an asset
  POST /assets/ingest   → trigger ingest for one or more symbols
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from db import repository as repo
from db.client import ensure_indexes
from ingest.pipeline import ingest_symbol

router = APIRouter(prefix="/assets", tags=["Assets"])


# ── schemas ────────────────────────────────────────────────────────────────────

class AssetSummary(BaseModel):
    id: str
    symbol: str
    asset_class: str
    region: str


class AssetDetail(BaseModel):
    id: str
    symbol: str
    asset_class: str
    description: str
    region: str
    attributes: dict
    created_at: str


class IngestRequest(BaseModel):
    symbols: list[str]
    period: str = "1y"
    interval: str = "1d"


# ── routes ─────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[AssetSummary], summary="Q1 – List all assets")
def list_assets(
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    """Return identification data for all financial assets in the warehouse."""
    docs = repo.list_assets(skip=skip, limit=limit)
    return [
        AssetSummary(
            id=d["_id"],
            symbol=d["symbol"],
            asset_class=d["asset_class"],
            region=d["region"],
        )
        for d in docs
    ]


@router.get("/{asset_id}", response_model=AssetDetail, summary="Q2 – Asset details")
def get_asset(asset_id: str):
    """Return all details of a financial asset by its identifier."""
    doc = repo.get_asset_by_id(asset_id)
    if not doc:
        raise HTTPException(404, f"Asset {asset_id!r} not found")
    return AssetDetail(
        id=doc["_id"],
        symbol=doc["symbol"],
        asset_class=doc["asset_class"],
        description=doc.get("description", ""),
        region=doc["region"],
        attributes=doc.get("attributes", {}),
        created_at=str(doc.get("created_at", "")),
    )


@router.post("/ingest", summary="Ingest asset data from Yahoo Finance")
def ingest_assets(body: IngestRequest):
    """
    Trigger data ingestion for one or more ticker symbols.
    Creates or updates the asset and appends new time-series points.
    """
    ensure_indexes()
    results = []
    for sym in body.symbols:
        result = ingest_symbol(sym.upper(), period=body.period, interval=body.interval)
        results.append(result)
    return {"results": results}
