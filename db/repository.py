"""
Temporal-aware repository layer.

Rules:
  - Records are NEVER updated or deleted in-place.
  - Asset metadata changes create a new AssetVersion document.
  - "Deleting" an asset adds a version with is_deleted=True.
  - Time-series points are append-only.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId

from db.client import (
    col_assets, col_asset_versions, col_sources,
    col_time_series, col_ts_points, col_ingestion_events,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _now() -> datetime:
    return datetime.now(timezone.utc)

def _sid() -> str:
    """Short unique string id."""
    return str(uuid.uuid4())

def _doc_id(doc: dict) -> str:
    return str(doc["_id"])


# ── Financial Assets ──────────────────────────────────────────────────────────

def create_asset(
    symbol: str,
    asset_class: str,
    description: str,
    region: str,
    extra_attributes: dict | None = None,
) -> dict:
    """Insert a new financial asset and its initial version."""
    now = _now()
    doc = {
        "symbol": symbol.upper(),
        "asset_class": asset_class,
        "description": description,
        "region": region,
        "attributes": extra_attributes or {},   # heterogeneous extra fields
        "created_at": now,
    }
    result = col_assets().insert_one(doc)
    asset_id = str(result.inserted_id)

    # create initial version
    _add_asset_version(asset_id, symbol, asset_class, description, region,
                       extra_attributes or {}, valid_from=now)
    doc["_id"] = asset_id
    return doc


def get_asset_by_id(asset_id: str) -> dict | None:
    doc = col_assets().find_one({"_id": ObjectId(asset_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


def get_asset_by_symbol(symbol: str) -> dict | None:
    doc = col_assets().find_one({"symbol": symbol.upper()})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


def list_assets(skip: int = 0, limit: int = 100) -> list[dict]:
    cursor = col_assets().find({}, {"symbol": 1, "asset_class": 1, "region": 1}).skip(skip).limit(limit)
    return [{**d, "_id": str(d["_id"])} for d in cursor]


def update_asset(asset_id: str, **fields) -> dict | None:
    """
    Temporal update: do NOT modify existing doc.
    Instead, update the root document fields AND append a new version.
    """
    asset = get_asset_by_id(asset_id)
    if not asset:
        return None

    now = _now()
    update_data = {k: v for k, v in fields.items() if v is not None}
    col_assets().update_one({"_id": ObjectId(asset_id)}, {"$set": update_data})

    merged = {**asset, **update_data}
    _add_asset_version(
        asset_id,
        merged.get("symbol", asset["symbol"]),
        merged.get("asset_class", asset["asset_class"]),
        merged.get("description", asset["description"]),
        merged.get("region", asset["region"]),
        merged.get("attributes", asset.get("attributes", {})),
        valid_from=now,
    )
    return get_asset_by_id(asset_id)


def delete_asset(asset_id: str) -> bool:
    """Temporal delete: add a marker version with is_deleted=True."""
    asset = get_asset_by_id(asset_id)
    if not asset:
        return False
    now = _now()
    _add_asset_version(
        asset_id,
        asset["symbol"], asset["asset_class"], asset["description"], asset["region"],
        asset.get("attributes", {}), valid_from=now, is_deleted=True,
    )
    return True


def _add_asset_version(
    asset_id: str, symbol: str, asset_class: str,
    description: str, region: str, attributes: dict,
    valid_from: datetime, is_deleted: bool = False,
) -> str:
    doc = {
        "asset_id": asset_id,
        "symbol": symbol,
        "asset_class": asset_class,
        "description": description,
        "region": region,
        "attributes": attributes,
        "valid_from": valid_from,
        "valid_to": None,         # open-ended until superseded
        "is_deleted": is_deleted,
    }
    result = col_asset_versions().insert_one(doc)
    return str(result.inserted_id)


def get_asset_at(asset_id: str, at: datetime) -> dict | None:
    """Return the asset version that was active at `at`."""
    doc = col_asset_versions().find_one(
        {
            "asset_id": asset_id,
            "valid_from": {"$lte": at},
            "$or": [{"valid_to": None}, {"valid_to": {"$gt": at}}],
            "is_deleted": False,
        },
        sort=[("valid_from", -1)],
    )
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


# ── Data Sources ──────────────────────────────────────────────────────────────

def upsert_source(name: str, api_endpoint: str, description: str = "") -> dict:
    now = _now()
    doc = {
        "name": name,
        "api_endpoint": api_endpoint,
        "description": description,
        "created_at": now,
    }
    result = col_sources().find_one_and_update(
        {"name": name},
        {"$setOnInsert": doc},
        upsert=True,
        return_document=True,
    )
    result["_id"] = str(result["_id"])
    return result


def get_source_by_id(source_id: str) -> dict | None:
    doc = col_sources().find_one({"_id": ObjectId(source_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


def list_sources() -> list[dict]:
    return [{**d, "_id": str(d["_id"])} for d in col_sources().find({})]


# ── Time Series ───────────────────────────────────────────────────────────────

def upsert_time_series(asset_id: str, source_id: str, frequency: str, currency: str) -> dict:
    doc = {
        "asset_id": asset_id,
        "source_id": source_id,
        "frequency": frequency,
        "currency": currency,
        "created_at": _now(),
    }
    result = col_time_series().find_one_and_update(
        {"asset_id": asset_id, "source_id": source_id},
        {"$setOnInsert": doc},
        upsert=True,
        return_document=True,
    )
    result["_id"] = str(result["_id"])
    return result


def get_time_series(series_id: str) -> dict | None:
    doc = col_time_series().find_one({"_id": ObjectId(series_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


def get_time_series_for_asset_source(asset_id: str, source_id: str) -> dict | None:
    doc = col_time_series().find_one({"asset_id": asset_id, "source_id": source_id})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


# ── Time Series Points (append-only) ─────────────────────────────────────────

def insert_ts_points(points: list[dict]) -> int:
    """
    Append time series points. Each dict must have:
      series_id, ingestion_id, timestamp, open, high, low, close, volume,
      and optionally extra_attributes (dict).
    Skips duplicates (series_id + timestamp) silently.
    """
    if not points:
        return 0
    from pymongo import UpdateOne
    ops = [
        UpdateOne(
            {"series_id": p["series_id"], "timestamp": p["timestamp"]},
            {"$setOnInsert": p},
            upsert=True,
        )
        for p in points
    ]
    result = col_ts_points().bulk_write(ops, ordered=False)
    return result.upserted_count


def get_ts_points(
    series_id: str,
    from_dt: datetime | None = None,
    to_dt: datetime | None = None,
    limit: int = 1000,
) -> list[dict]:
    query: dict[str, Any] = {"series_id": series_id}
    if from_dt or to_dt:
        ts_filter: dict[str, Any] = {}
        if from_dt:
            ts_filter["$gte"] = from_dt
        if to_dt:
            ts_filter["$lte"] = to_dt
        query["timestamp"] = ts_filter

    cursor = (
        col_ts_points()
        .find(query, {"_id": 0})
        .sort("timestamp", 1)
        .limit(limit)
    )
    return list(cursor)


# ── Ingestion Events ──────────────────────────────────────────────────────────

def create_ingestion_event(source_id: str, request_params: dict) -> dict:
    doc = {
        "source_id": source_id,
        "ingestion_time": _now(),
        "request_params": request_params,
        "status": "running",
        "points_inserted": 0,
        "error": None,
    }
    result = col_ingestion_events().insert_one(doc)
    doc["_id"] = str(result.inserted_id)
    return doc


def finish_ingestion_event(event_id: str, points_inserted: int, error: str | None = None) -> None:
    col_ingestion_events().update_one(
        {"_id": ObjectId(event_id)},
        {"$set": {
            "status": "failed" if error else "completed",
            "points_inserted": points_inserted,
            "error": error,
            "finished_at": _now(),
        }},
    )
