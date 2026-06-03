"""
Shared pytest fixtures.
Uses mongomock to replace every MongoDB collection in db.repository
without needing a real MongoDB instance.
"""
import pytest

try:
    import mongomock
    HAS_MONGOMOCK = True
except ImportError:
    HAS_MONGOMOCK = False


@pytest.fixture(autouse=True)
def mongo_mock(monkeypatch):
    if not HAS_MONGOMOCK:
        pytest.skip("mongomock not installed — run: pip install mongomock")

    client = mongomock.MongoClient()
    db = client["test_acme_dw"]

    import db.repository as repo

    monkeypatch.setattr(repo, "col_assets",           lambda: db["financial_assets"])
    monkeypatch.setattr(repo, "col_asset_versions",   lambda: db["asset_versions"])
    monkeypatch.setattr(repo, "col_sources",          lambda: db["data_sources"])
    monkeypatch.setattr(repo, "col_time_series",      lambda: db["time_series"])
    monkeypatch.setattr(repo, "col_ts_points",        lambda: db["time_series_points"])
    monkeypatch.setattr(repo, "col_ingestion_events", lambda: db["ingestion_events"])

    import db.client as db_client
    monkeypatch.setattr(db_client, "ensure_indexes", lambda: None)

    yield db
