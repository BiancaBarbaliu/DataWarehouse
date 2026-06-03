"""
Unit tests for ingest/pipeline.py.
Mocks Yahoo Finance so no real network calls are made.
MongoDB is mocked via conftest.py fixture.

Run with:
    python3.11 -m pytest tests/ -v
"""
from datetime import datetime
from unittest.mock import patch


# ── Mock data ─────────────────────────────────────────────────────────────────

MOCK_TICKER_INFO = {
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "asset_class": "stock",
    "description": "Apple designs and sells consumer electronics.",
    "region": "US",
    "currency": "USD",
    "extra_attributes": {"sector": "Technology", "industry": "Consumer Electronics",
                         "exchange": "NASDAQ", "market_cap": 3000000000000},
}

MOCK_HISTORY = [
    {"timestamp": datetime(2024, 1, 1), "open": 185.0, "high": 188.0,
     "low": 184.0, "close": 187.0, "volume": 50000000, "extra_attributes": {}},
    {"timestamp": datetime(2024, 1, 2), "open": 187.0, "high": 190.0,
     "low": 186.0, "close": 189.0, "volume": 48000000, "extra_attributes": {}},
    {"timestamp": datetime(2024, 1, 3), "open": 189.0, "high": 192.0,
     "low": 188.0, "close": 191.0, "volume": 52000000, "extra_attributes": {}},
]


class TestIngestSymbol:
    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_creates_asset(self, mock_hist, mock_info):
        from ingest.pipeline import ingest_symbol
        from db.repository import get_asset_by_symbol
        ingest_symbol("AAPL", period="1y")
        asset = get_asset_by_symbol("AAPL")
        assert asset is not None
        assert asset["symbol"] == "AAPL"

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_creates_data_source(self, mock_hist, mock_info):
        from ingest.pipeline import ingest_symbol
        from db.repository import list_sources
        ingest_symbol("AAPL", period="1y")
        sources = list_sources()
        assert any("Yahoo" in s["name"] for s in sources)

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_stores_correct_point_count(self, mock_hist, mock_info):
        from ingest.pipeline import ingest_symbol
        result = ingest_symbol("AAPL", period="1y")
        assert result["total_fetched"] == 3
        assert result["new_points"] == 3

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_idempotent_second_run(self, mock_hist, mock_info):
        """Running ingest twice should not create duplicate points."""
        from ingest.pipeline import ingest_symbol
        ingest_symbol("AAPL", period="1y")
        result2 = ingest_symbol("AAPL", period="1y")
        # second run inserts 0 new points (all duplicates)
        assert result2["new_points"] == 0

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_records_provenance(self, mock_hist, mock_info, mongo_mock):
        """Every ingestion should create an IngestionEvent linked to source."""
        from ingest.pipeline import ingest_symbol
        result = ingest_symbol("AAPL", period="1y")
        events = list(mongo_mock["ingestion_events"].find({"source_id": result["source_id"]}))
        assert len(events) >= 1
        assert events[0]["status"] == "completed"

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", side_effect=Exception("Network error"))
    def test_ingest_handles_fetch_failure(self, mock_hist, mock_info):
        """If Yahoo Finance fetch fails, result should be 'failed' not an exception."""
        from ingest.pipeline import ingest_symbol
        result = ingest_symbol("AAPL", period="1y")
        assert result["status"] == "failed"
        assert "error" in result

    @patch("ingest.pipeline.fetch_ticker_info", return_value=MOCK_TICKER_INFO)
    @patch("ingest.pipeline.fetch_history", return_value=MOCK_HISTORY)
    def test_ingest_returns_summary_dict(self, mock_hist, mock_info):
        from ingest.pipeline import ingest_symbol
        result = ingest_symbol("AAPL")
        assert "asset_id" in result
        assert "series_id" in result
        assert "source_id" in result
        assert "ingestion_id" in result
        assert result["status"] == "completed"
