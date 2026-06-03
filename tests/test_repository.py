"""
Unit tests for db/repository.py (DAL layer).
Uses mongomock via conftest.py fixture.

Run with:
    python3.11 -m pytest tests/ -v
"""
from datetime import datetime


# ── DAL: Asset CRUD ───────────────────────────────────────────────────────────

class TestCreateAsset:
    def test_create_returns_asset_with_id(self):
        from db.repository import create_asset
        asset = create_asset("AAPL", "stock", "Apple Inc.", "US")
        assert asset["symbol"] == "AAPL"
        assert "_id" in asset
        assert asset["asset_class"] == "stock"

    def test_create_also_creates_initial_version(self, mongo_mock):
        from db.repository import create_asset
        asset = create_asset("MSFT", "stock", "Microsoft", "US")
        versions = list(mongo_mock["asset_versions"].find({"asset_id": asset["_id"]}))
        assert len(versions) == 1
        assert versions[0]["is_deleted"] is False

    def test_create_stores_extra_attributes(self):
        from db.repository import create_asset
        asset = create_asset("BTC-USD", "crypto", "Bitcoin", "Global",
                             extra_attributes={"exchange": "Coinbase"})
        assert asset["attributes"]["exchange"] == "Coinbase"


class TestGetAsset:
    def test_get_by_id_returns_correct_asset(self):
        from db.repository import create_asset, get_asset_by_id
        asset = create_asset("GOOGL", "stock", "Alphabet", "US")
        fetched = get_asset_by_id(asset["_id"])
        assert fetched is not None
        assert fetched["symbol"] == "GOOGL"

    def test_get_by_symbol_returns_correct_asset(self):
        from db.repository import create_asset, get_asset_by_symbol
        create_asset("TSLA", "stock", "Tesla Inc.", "US")
        fetched = get_asset_by_symbol("TSLA")
        assert fetched is not None
        assert fetched["symbol"] == "TSLA"

    def test_get_nonexistent_returns_none(self):
        from db.repository import get_asset_by_id
        result = get_asset_by_id("000000000000000000000000")
        assert result is None


class TestListAssets:
    def test_list_returns_all_assets(self):
        from db.repository import create_asset, list_assets
        create_asset("AAPL", "stock", "Apple", "US")
        create_asset("BTC-USD", "crypto", "Bitcoin", "Global")
        assets = list_assets()
        symbols = [a["symbol"] for a in assets]
        assert "AAPL" in symbols
        assert "BTC-USD" in symbols

    def test_list_respects_limit(self):
        from db.repository import create_asset, list_assets
        for i in range(5):
            create_asset(f"SYM{i}", "stock", f"Company {i}", "US")
        assets = list_assets(limit=3)
        assert len(assets) <= 3

    def test_list_empty_db_returns_empty(self):
        from db.repository import list_assets
        assert list_assets() == []


# ── DAL: Temporal Versioning ──────────────────────────────────────────────────

class TestTemporalVersioning:
    def test_update_creates_new_version(self, mongo_mock):
        from db.repository import create_asset, update_asset
        asset = create_asset("AMZN", "stock", "Amazon", "US")
        update_asset(asset["_id"], description="Amazon.com Inc.")
        versions = list(mongo_mock["asset_versions"].find({"asset_id": asset["_id"]}))
        assert len(versions) == 2  # initial + update

    def test_update_does_not_modify_original_version(self, mongo_mock):
        from db.repository import create_asset, update_asset
        asset = create_asset("NFLX", "stock", "Netflix", "US")
        original_version = list(mongo_mock["asset_versions"].find({"asset_id": asset["_id"]}))[0]
        update_asset(asset["_id"], description="Netflix Inc.")
        assert original_version["description"] == "Netflix"

    def test_delete_creates_marker_version(self, mongo_mock):
        from db.repository import create_asset, delete_asset
        asset = create_asset("META", "stock", "Meta Platforms", "US")
        result = delete_asset(asset["_id"])
        assert result is True
        versions = list(mongo_mock["asset_versions"].find({"asset_id": asset["_id"]}))
        deleted_versions = [v for v in versions if v["is_deleted"]]
        assert len(deleted_versions) == 1

    def test_delete_does_not_remove_document(self):
        from db.repository import create_asset, delete_asset, get_asset_by_id
        asset = create_asset("SPOT", "stock", "Spotify", "Europe")
        delete_asset(asset["_id"])
        # document still exists in collection
        doc = get_asset_by_id(asset["_id"])
        assert doc is not None


# ── DAL: Time Series Points ───────────────────────────────────────────────────

class TestTimeSeriesPoints:
    def _make_point(self, series_id, ts, close=100.0):
        return {
            "series_id": series_id,
            "ingestion_id": "test_ingest_1",
            "timestamp": ts,
            "open": close - 1,
            "high": close + 2,
            "low": close - 2,
            "close": close,
            "volume": 1000000,
            "extra_attributes": {},
        }

    def test_insert_points_returns_count(self):
        from db.repository import insert_ts_points
        points = [self._make_point("series1", datetime(2024, 1, i + 1)) for i in range(5)]
        inserted = insert_ts_points(points)
        assert inserted == 5

    def test_duplicate_timestamps_ignored(self):
        """Idempotency: inserting same timestamp twice should not create duplicates."""
        from db.repository import insert_ts_points, get_ts_points
        ts = datetime(2024, 6, 1)
        point = self._make_point("series2", ts, close=150.0)
        insert_ts_points([point])
        insert_ts_points([point])  # duplicate
        results = get_ts_points("series2")
        assert len(results) == 1

    def test_get_ts_points_returns_sorted(self):
        from db.repository import insert_ts_points, get_ts_points
        points = [self._make_point("series3", datetime(2024, 1, i + 1)) for i in range(3, -1, -1)]
        insert_ts_points(points)
        results = get_ts_points("series3")
        timestamps = [r["timestamp"] for r in results]
        assert timestamps == sorted(timestamps)

    def test_get_ts_points_date_filter(self):
        from db.repository import insert_ts_points, get_ts_points
        points = [self._make_point("series4", datetime(2024, 1, i + 1)) for i in range(10)]
        insert_ts_points(points)
        results = get_ts_points("series4",
                                from_dt=datetime(2024, 1, 3),
                                to_dt=datetime(2024, 1, 7))
        for r in results:
            assert datetime(2024, 1, 3) <= r["timestamp"] <= datetime(2024, 1, 7)

    def test_insert_empty_list_returns_zero(self):
        from db.repository import insert_ts_points
        assert insert_ts_points([]) == 0


# ── DAL: Data Sources ─────────────────────────────────────────────────────────

class TestDataSources:
    def test_upsert_source_creates_new(self):
        from db.repository import upsert_source, list_sources
        upsert_source("Yahoo Finance", "https://finance.yahoo.com", "Yahoo")
        sources = list_sources()
        assert any(s["name"] == "Yahoo Finance" for s in sources)

    def test_upsert_source_idempotent(self):
        from db.repository import upsert_source, list_sources
        upsert_source("Bloomberg", "https://bloomberg.com", "Bloomberg")
        upsert_source("Bloomberg", "https://bloomberg.com", "Bloomberg")
        sources = [s for s in list_sources() if s["name"] == "Bloomberg"]
        assert len(sources) == 1

    def test_get_source_by_id(self):
        from db.repository import upsert_source, get_source_by_id
        source = upsert_source("Nasdaq", "https://data.nasdaq.com", "Nasdaq Data Link")
        fetched = get_source_by_id(source["_id"])
        assert fetched is not None
        assert fetched["name"] == "Nasdaq"


# ── DAL: Ingestion Events ─────────────────────────────────────────────────────

class TestIngestionEvents:
    def test_create_event_has_running_status(self):
        from db.repository import create_ingestion_event
        event = create_ingestion_event("source1", {"symbol": "AAPL"})
        assert event["status"] == "running"
        assert event["points_inserted"] == 0

    def test_finish_event_marks_completed(self, mongo_mock):
        from db.repository import create_ingestion_event, finish_ingestion_event
        from bson import ObjectId
        event = create_ingestion_event("source1", {"symbol": "AAPL"})
        finish_ingestion_event(event["_id"], points_inserted=250)
        updated = mongo_mock["ingestion_events"].find_one({"_id": ObjectId(event["_id"])})
        assert updated["status"] == "completed"
        assert updated["points_inserted"] == 250

    def test_finish_event_with_error_marks_failed(self, mongo_mock):
        from db.repository import create_ingestion_event, finish_ingestion_event
        from bson import ObjectId
        event = create_ingestion_event("source1", {"symbol": "BAD"})
        finish_ingestion_event(event["_id"], points_inserted=0, error="Network timeout")
        updated = mongo_mock["ingestion_events"].find_one({"_id": ObjectId(event["_id"])})
        assert updated["status"] == "failed"
        assert updated["error"] == "Network timeout"
