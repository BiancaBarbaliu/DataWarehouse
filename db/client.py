"""MongoDB client and collection accessors."""
import os
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
_MONGO_DB = os.getenv("MONGO_DB", "acme_dw")

_client: MongoClient | None = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(_MONGO_URI)
    return _client


def get_db():
    return get_client()[_MONGO_DB]


# ---------- collection helpers ----------

def col_assets() -> Collection:
    return get_db()["financial_assets"]

def col_asset_versions() -> Collection:
    return get_db()["asset_versions"]

def col_sources() -> Collection:
    return get_db()["data_sources"]

def col_time_series() -> Collection:
    return get_db()["time_series"]

def col_ts_points() -> Collection:
    return get_db()["time_series_points"]

def col_ingestion_events() -> Collection:
    return get_db()["ingestion_events"]


def ensure_indexes() -> None:
    """Create all performance-critical indexes (idempotent)."""
    # financial_assets
    col_assets().create_index([("symbol", ASCENDING)], unique=True)
    col_assets().create_index([("asset_class", ASCENDING)])

    # asset_versions – temporal queries
    col_asset_versions().create_index([("asset_id", ASCENDING), ("valid_from", DESCENDING)])

    # data_sources
    col_sources().create_index([("name", ASCENDING)], unique=True)

    # time_series
    col_time_series().create_index(
        [("asset_id", ASCENDING), ("source_id", ASCENDING)], unique=True
    )

    # time_series_points – the hot path for Q5
    col_ts_points().create_index(
        [("series_id", ASCENDING), ("timestamp", ASCENDING)]
    )
    col_ts_points().create_index([("ingestion_id", ASCENDING)])

    # ingestion_events
    col_ingestion_events().create_index([("source_id", ASCENDING), ("ingestion_time", DESCENDING)])
