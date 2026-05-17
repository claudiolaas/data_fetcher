"""Tests for data_fetcher Phase 1: crypto OHLCV SQLite CLI."""

import sqlite3
import tempfile
from pathlib import Path
from typing import List
from unittest.mock import Mock, patch

import pytest
import pandas as pd
from typer.testing import CliRunner

from data_fetcher.cli import app
from data_fetcher.data import BaseDataFetcher
from data_fetcher.storage.sqlite import SQLiteStore

runner = CliRunner()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_db() -> str:
    """Create a temporary SQLite database path."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    yield db_path
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def store(temp_db: str) -> SQLiteStore:
    return SQLiteStore(temp_db)


@pytest.fixture
def sample_ohlcv_rows() -> List[tuple]:
    """Return a list of 3 OHLCV rows for binance BTC/USDT 1h."""
    return [
        (
            1704067200000,  # milliseconds (2024-01-01T00:00:00Z)
            "2024-01-01T00:00:00Z",
            "binance",
            "BTC/USDT",
            "1h",
            42000.0,  # open
            42100.0,  # high
            41900.0,  # low
            42050.0,  # price (close)
            100.5,    # volume
        ),
        (
            1704070800000,  # 2024-01-01T01:00:00Z
            "2024-01-01T01:00:00Z",
            "binance",
            "BTC/USDT",
            "1h",
            42050.0,
            42200.0,
            42000.0,
            42150.0,
            150.2,
        ),
        (
            1704074400000,  # 2024-01-01T02:00:00Z
            "2024-01-01T02:00:00Z",
            "binance",
            "BTC/USDT",
            "1h",
            42150.0,
            42300.0,
            42100.0,
            42200.0,
            200.0,
        ),
    ]


# ---------------------------------------------------------------------------
# SQLite Store Tests
# ---------------------------------------------------------------------------


class TestSQLiteStoreSchema:
    def test_schema_creation(self, temp_db: str) -> None:
        """Verify the price_data table is created with the correct schema."""
        SQLiteStore(temp_db)  # triggers schema creation / dir creation
        conn = sqlite3.connect(temp_db)
        cursor = conn.execute("PRAGMA table_info(price_data)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        conn.close()

        assert "milliseconds" in columns
        assert "timestamp" in columns
        assert "exchange" in columns
        assert "symbol" in columns
        assert "timeframe" in columns
        assert "open" in columns
        assert "high" in columns
        assert "low" in columns
        assert "price" in columns
        assert "volume" in columns
        assert columns["milliseconds"] == "INTEGER"
        assert columns["price"] == "REAL"
        assert columns["volume"] == "REAL"

    def test_unique_constraint(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify that inserting the same row twice is idempotent."""
        first = store.insert_ohlcv(sample_ohlcv_rows)
        second = store.insert_ohlcv(sample_ohlcv_rows)
        assert first == 3  # First insert adds 3 rows
        assert second == 0  # Second insert adds 0 (all duplicates)

        conn = sqlite3.connect(store.db_path)
        count = conn.execute("SELECT COUNT(*) FROM price_data").fetchone()[0]
        conn.close()
        assert count == 3


class TestSQLiteStoreInsertAndResume:
    def test_insert_and_count(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify inserted rows can be counted."""
        store.insert_ohlcv(sample_ohlcv_rows)
        conn = sqlite3.connect(store.db_path)
        count = conn.execute("SELECT COUNT(*) FROM price_data").fetchone()[0]
        conn.close()
        assert count == 3

    def test_get_max_timestamp(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify max timestamp lookup for resume."""
        store.insert_ohlcv(sample_ohlcv_rows)
        max_ts = store.get_max_timestamp("binance", "BTC/USDT", "1h")
        assert max_ts == 1704074400000  # last row's ms

    def test_get_max_timestamp_empty(self, store: SQLiteStore) -> None:
        """Verify max timestamp is None when no data exists."""
        max_ts = store.get_max_timestamp("binance", "NONEXISTENT", "1h")
        assert max_ts is None


class TestSQLiteStoreInventory:
    def test_inventory_summary(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify inventory returns correct counts."""
        store.insert_ohlcv(sample_ohlcv_rows)
        inventory = store.get_inventory()
        assert len(inventory) == 1
        row = inventory[0]
        assert row.exchange == "binance"
        assert row.symbol == "BTC/USDT"
        assert row.timeframe == "1h"
        assert row.rows == 3
        assert row.first_datetime_utc == "2024-01-01T00:00:00Z"
        assert row.last_datetime_utc == "2024-01-01T02:00:00Z"

    def test_inventory_filter(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify inventory filtering by exchange."""
        store.insert_ohlcv(sample_ohlcv_rows)
        inventory = store.get_inventory(exchange="binance")
        assert len(inventory) == 1
        inventory = store.get_inventory(exchange="coinbase")
        assert len(inventory) == 0


class TestSQLiteStoreDelete:
    def test_delete_for_key(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify delete removes all rows for a key."""
        store.insert_ohlcv(sample_ohlcv_rows)
        deleted = store.delete_for_key("binance", "BTC/USDT", "1h")
        assert deleted == 3
        conn = sqlite3.connect(store.db_path)
        count = conn.execute("SELECT COUNT(*) FROM price_data").fetchone()[0]
        conn.close()
        assert count == 0


# ---------------------------------------------------------------------------
# Validation Tests
# ---------------------------------------------------------------------------


class TestValidation:
    def test_validate_passes_clean_data(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify validation passes on clean data."""
        store.insert_ohlcv(sample_ohlcv_rows)
        results = store.validate()
        assert len(results) == 1
        r = results[0]
        assert r.status == "PASS"
        assert r.duplicate_count == 0
        assert r.null_count == 0
        assert r.non_positive_price_count == 0
        assert r.non_positive_volume_count == 0
        assert r.gap_count == 0

    def test_validate_detects_missing_hourly_bar(self, store: SQLiteStore) -> None:
        """Verify validation detects a gap in hourly data."""
        rows = [
            (1704067200000, "2024-01-01T00:00:00Z", "binance", "BTC/USDT", "1h", 100, 101, 99, 100.5, 10),
            # Missing 01:00 bar
            (1704074400000, "2024-01-01T02:00:00Z", "binance", "BTC/USDT", "1h", 101, 102, 100, 101.5, 15),
        ]
        store.insert_ohlcv(rows)
        results = store.validate()
        assert len(results) == 1
        r = results[0]
        assert r.gap_count >= 1
        assert r.status == "WARN"

    def test_validate_detects_non_positive_price(self, store: SQLiteStore) -> None:
        """Verify validation flags non-positive price."""
        rows = [
            (1704067200000, "2024-01-01T00:00:00Z", "binance", "BTC/USDT", "1h", 100, 101, 99, 0.0, 10),
        ]
        store.insert_ohlcv(rows)
        results = store.validate()
        assert len(results) == 1
        r = results[0]
        assert r.non_positive_price_count >= 1
        assert r.status == "FAIL"

    def test_validate_detects_non_positive_volume(self, store: SQLiteStore) -> None:
        """Verify validation flags non-positive volume."""
        rows = [
            (1704067200000, "2024-01-01T00:00:00Z", "binance", "BTC/USDT", "1h", 100, 101, 99, 100.5, 0),
        ]
        store.insert_ohlcv(rows)
        results = store.validate()
        assert len(results) == 1
        r = results[0]
        assert r.non_positive_volume_count >= 1
        assert r.status == "FAIL"


# ---------------------------------------------------------------------------
# Symbol Filtering Tests
# ---------------------------------------------------------------------------


class TestSymbolFiltering:
    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_get_symbols_quote_filter(self, mock_create: Mock) -> None:
        """Verify symbol filtering by quote currency."""
        mock_exchange = Mock()
        mock_exchange.markets = {
            "BTC/USDT": {"base": "BTC", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
            "ETH/USDT": {"base": "ETH", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
            "BTC/EUR": {"base": "BTC", "quote": "EUR", "active": True, "type": "spot", "limits": {}, "precision": {}},
        }
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher
        fetcher = CryptoDataFetcher("test")
        results = fetcher.get_symbols(quote="USDT")
        assert len(results) == 2
        assert all(r["quote"] == "USDT" for r in results)

    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_get_symbols_inactive_filter(self, mock_create: Mock) -> None:
        """Verify filtering out inactive symbols."""
        mock_exchange = Mock()
        mock_exchange.markets = {
            "BTC/USDT": {"base": "BTC", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
            "ETH/USDT": {"base": "ETH", "quote": "USDT", "active": False, "type": "spot", "limits": {}, "precision": {}},
        }
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher
        fetcher = CryptoDataFetcher("test")
        results = fetcher.get_symbols(active_only=True)
        assert len(results) == 1
        assert results[0]["symbol"] == "BTC/USDT"

    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_get_symbols_spot_only(self, mock_create: Mock) -> None:
        """Verify filtering out non-spot markets."""
        mock_exchange = Mock()
        mock_exchange.markets = {
            "BTC/USDT": {"base": "BTC", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
            "BTC/USDT:USDT": {"base": "BTC", "quote": "USDT", "active": True, "type": "swap", "limits": {}, "precision": {}},
        }
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher
        fetcher = CryptoDataFetcher("test")
        results = fetcher.get_symbols(spot_only=True)
        assert len(results) == 1
        assert results[0]["type"] == "spot"


# ---------------------------------------------------------------------------
# Fetch Loop Tests (mocked)
# ---------------------------------------------------------------------------


class TestFetchLoop:
    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_fetch_ohlcv_pagination(self, mock_create: Mock) -> None:
        """Verify fetch_ohlcv paginates correctly."""
        mock_exchange = Mock()
        # Return full pages then a partial page
        mock_exchange.fetch_ohlcv.side_effect = [
            [[i * 3600000, 100.0, 101.0, 99.0, 100.5, 10.0] for i in range(1000)],
            [[(i + 1000) * 3600000, 101.0, 102.0, 100.0, 101.5, 15.0] for i in range(500)],
        ]
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher
        fetcher = CryptoDataFetcher("test")
        candles = fetcher.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1000)
        assert len(candles) == 1500

    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_fetch_ohlcv_max_requests(self, mock_create: Mock) -> None:
        """Verify max_requests stops fetching early."""
        mock_exchange = Mock()
        mock_exchange.fetch_ohlcv.return_value = [
            [i * 3600000, 100.0, 101.0, 99.0, 100.5, 10.0] for i in range(1000)
        ]
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher
        fetcher = CryptoDataFetcher("test")
        candles = fetcher.fetch_ohlcv("BTC/USDT", "1h", since=0, limit=1000, max_requests=1)
        assert len(candles) == 1000
        assert mock_exchange.fetch_ohlcv.call_count == 1

    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_fetch_ohlcv_filters_until_boundary(self, mock_create: Mock) -> None:
        """Verify candles after until are not returned or inserted by callers."""
        mock_exchange = Mock()
        mock_exchange.fetch_ohlcv.return_value = [
            [0, 100.0, 101.0, 99.0, 100.5, 10.0],
            [3_600_000, 101.0, 102.0, 100.0, 101.5, 15.0],
            [7_200_000, 102.0, 103.0, 101.0, 102.5, 20.0],
        ]
        mock_create.return_value = mock_exchange

        from data_fetcher.providers.crypto import CryptoDataFetcher

        fetcher = CryptoDataFetcher("test")
        candles = fetcher.fetch_ohlcv(
            "BTC/USDT",
            "1h",
            since=0,
            until=3_600_000,
            limit=1000,
        )
        assert [row[0] for row in candles] == [0, 3_600_000]


# ---------------------------------------------------------------------------
# Legacy CSV Cache Tests
# ---------------------------------------------------------------------------


class DummyDataFetcher(BaseDataFetcher):
    def get_data(self) -> pd.DataFrame:
        return pd.DataFrame()

    def get_markets(self) -> dict:
        return {}

    def get_ticker(self) -> list[str]:
        return []


class TestLegacyCsvCacheNaming:
    def test_build_actual_date_filename_uses_returned_data_dates(self) -> None:
        fetcher = DummyDataFetcher()
        frame = pd.DataFrame(
            {
                "dt": [
                    "2023-04-21 04:00:00+00:00",
                    "2025-01-03 05:00:00+00:00",
                ]
            }
        )

        filename = fetcher.build_actual_date_filename(
            frame,
            ticker="AACT.U",
            step="1d",
            fallback_start="1800-01-01",
            fallback_end="2025-01-05",
        )

        assert filename == "AACT.U_1d_2023-04-21_2025-01-03.csv"

    def test_build_actual_date_filename_falls_back_without_dates(self) -> None:
        fetcher = DummyDataFetcher()
        filename = fetcher.build_actual_date_filename(
            pd.DataFrame(),
            ticker="AACT.U",
            step="1d",
            fallback_start="1800-01-01",
            fallback_end="2025-01-05",
        )

        assert filename == "AACT.U_1d_1800-01-01_2025-01-05.csv"


# ---------------------------------------------------------------------------
# CLI Smoke Tests
# ---------------------------------------------------------------------------


class TestCLI:
    def test_help(self) -> None:
        """Verify CLI help output."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Usage:" in result.stdout

    @patch("data_fetcher.providers.crypto.create_exchange")
    def test_symbols_command(self, mock_create: Mock) -> None:
        """Verify symbols command produces output."""
        mock_exchange = Mock()
        mock_exchange.markets = {
            "BTC/USDT": {"base": "BTC", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
            "ETH/USDT": {"base": "ETH", "quote": "USDT", "active": True, "type": "spot", "limits": {}, "precision": {}},
        }
        mock_create.return_value = mock_exchange

        result = runner.invoke(app, ["symbols", "--exchange", "binance", "--quote", "USDT"])
        assert result.exit_code == 0
        assert "BTC/USDT" in result.stdout
        assert "ETH/USDT" in result.stdout

    def test_inventory_no_db(self) -> None:
        """Verify inventory on non-existent database (should create but be empty)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "empty.db"
            result = runner.invoke(app, ["inventory", "--db-path", str(db_path)])
            assert result.exit_code == 0
            assert "No data found" in result.stdout

    def test_inventory_with_data(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify inventory shows stored data."""
        store.insert_ohlcv(sample_ohlcv_rows)
        result = runner.invoke(app, ["inventory", "--db-path", store.db_path])
        assert result.exit_code == 0
        assert "BTC/USDT" in result.stdout
        assert "3" in result.stdout  # 3 rows

    def test_validate_with_data(self, store: SQLiteStore, sample_ohlcv_rows: List[tuple]) -> None:
        """Verify validate runs on data with no issues."""
        store.insert_ohlcv(sample_ohlcv_rows)
        result = runner.invoke(app, ["validate", "--db-path", store.db_path])
        assert result.exit_code == 0
        assert "PASS" in result.stdout

    def test_validate_detects_issue(self, store: SQLiteStore) -> None:
        """Verify validate flags bad data."""
        rows = [
            (1704067200000, "2024-01-01T00:00:00Z", "binance", "BTC/USDT", "1h", 100, 101, 99, 0.0, 10),
        ]
        store.insert_ohlcv(rows)
        result = runner.invoke(app, ["validate", "--db-path", store.db_path])
        assert result.exit_code == 0
        assert "FAIL" in result.stdout or "WARN" in result.stdout

    def test_exchanges_command(self) -> None:
        """Verify exchanges command lists supported exchanges."""
        result = runner.invoke(app, ["exchanges"])
        assert result.exit_code == 0
        assert "Binance" in result.stdout
        assert "has_fetch_ohlcv" in result.stdout


# ---------------------------------------------------------------------------
# Live Smoke Test (skipped by default)
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="Hits live exchange; run manually to verify")
class TestLiveSmoke:
    def test_fetch_one_symbol_one_request(self, temp_db: str) -> None:
        """Fetch BTC/USDT 1h with max one request to verify integration."""
        result = runner.invoke(app, [
            "fetch",
            "--exchange", "binance",
            "--symbols", "BTC/USDT",
            "--timeframe", "1h",
            "--since", "earliest",
            "--db-path", temp_db,
            "--max-requests-per-symbol", "1",
        ])
        assert result.exit_code == 0
        assert "Inserted" in result.stdout or "No data" in result.stdout
