"""SQLite persistence layer for OHLCV data."""

import logging
import re
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from data_fetcher.models import InventoryRow, ValidationResult

logger = logging.getLogger(__name__)

PRICE_FRAME_COLUMNS = ["milliseconds", "timestamp", "symbol", "price", "volume"]
PRICE_FRAME_KEY_COLUMNS = ["exchange", "timeframe"]


#: Canonical schema for the price_data table. ``price`` stores the candle
#: close. ``open``, ``high``, and ``low`` are preserved for consumers that need
#: full OHLCV context.
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS price_data (
    milliseconds INTEGER NOT NULL,
    timestamp    TEXT    NOT NULL,
    exchange     TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    timeframe    TEXT    NOT NULL,
    open         REAL    NOT NULL,
    high         REAL    NOT NULL,
    low          REAL    NOT NULL,
    price        REAL    NOT NULL,
    volume       REAL    NOT NULL,
    UNIQUE(exchange, symbol, timeframe, milliseconds)
);
"""

#: Index on exchange/symbol/timeframe for fast lookups.
CREATE_INDEX_SYMBOL_SQL = """
CREATE INDEX IF NOT EXISTS idx_price_data_symbol
ON price_data(exchange, symbol, timeframe);
"""

#: Index on milliseconds for time-range queries.
CREATE_INDEX_TIME_SQL = """
CREATE INDEX IF NOT EXISTS idx_price_data_time
ON price_data(milliseconds);
"""


class SQLiteStore:
    """SQLite storage for OHLCV data.

    Manages the ``price_data`` table with idempotent inserts, resume
    capability, inventory queries, and validation.
    """

    def __init__(self, db_path: str):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        """Create the price_data table and indexes if they do not exist.

        Creates parent directories if they do not exist.
        """
        db_dir = Path(self.db_path).parent
        db_dir.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            conn.execute(CREATE_TABLE_SQL)
            conn.execute(CREATE_INDEX_SYMBOL_SQL)
            conn.execute(CREATE_INDEX_TIME_SQL)
            conn.commit()
        finally:
            conn.close()

    def get_max_timestamp(
        self, exchange: str, symbol: str, timeframe: str
    ) -> Optional[int]:
        """Return the maximum stored milliseconds for a given key.

        Used to resume fetching from the latest stored candle.
        Returns ``None`` when no data exists for the key.
        """
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT MAX(milliseconds) AS max_ms
                FROM price_data
                WHERE exchange = ? AND symbol = ? AND timeframe = ?
                """,
                (exchange, symbol, timeframe),
            ).fetchone()
            return row["max_ms"] if row and row["max_ms"] is not None else None
        finally:
            conn.close()

    def insert_ohlcv(self, rows: List[Tuple]) -> int:
        """Insert OHLCV rows idempotently.

        Parameters
        ----------
        rows : list of tuple
            Each tuple must have 10 fields matching the schema order:
            (milliseconds, timestamp, exchange, symbol, timeframe,
             open, high, low, price, volume).

        Returns
        -------
        int
            Number of rows inserted (ignoring duplicates).
        """
        if not rows:
            return 0

        conn = self._connect()
        try:
            conn.executemany(
                """
                INSERT OR IGNORE INTO price_data
                (milliseconds, timestamp, exchange, symbol, timeframe,
                 open, high, low, price, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            return conn.total_changes
        finally:
            conn.close()

    def delete_for_key(
        self, exchange: str, symbol: str, timeframe: str
    ) -> int:
        """Delete all rows matching exchange/symbol/timeframe.

        Used by ``--overwrite`` mode.
        Returns the number of rows deleted.
        """
        conn = self._connect()
        try:
            cursor = conn.execute(
                "DELETE FROM price_data WHERE exchange = ? AND symbol = ? AND timeframe = ?",
                (exchange, symbol, timeframe),
            )
            conn.commit()
            return cursor.rowcount
        finally:
            conn.close()

    def load_price_frame(
        self,
        symbol: str,
        timeframe: Optional[str],
        exchange: Optional[str] = "binance",
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_key_columns: bool = False,
    ) -> pd.DataFrame:
        """Return backtesting-compatible price rows for one symbol.

        The returned frame contains ``milliseconds``, ``timestamp``, ``symbol``,
        ``price`` (the candle close), and ``volume`` by default. Rows are
        ordered by ``milliseconds``. Time bounds are inclusive.

        Passing ``exchange=None`` or ``timeframe=None`` is allowed only when the
        matching rows are unambiguous for that dimension.
        """
        return self.load_prices(
            symbols=[symbol],
            timeframe=timeframe,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
            include_key_columns=include_key_columns,
        )

    def load_prices(
        self,
        symbols: Optional[List[str]] = None,
        timeframe: Optional[str] = None,
        exchange: Optional[str] = None,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        include_key_columns: bool = False,
    ) -> pd.DataFrame:
        """Return backtesting-compatible price rows.

        Empty matches return an empty DataFrame with the expected columns.
        Reads that would mix multiple exchanges or timeframes raise
        ``ValueError`` unless the ambiguous dimension is explicitly filtered.
        """
        select_columns = list(PRICE_FRAME_COLUMNS)
        if include_key_columns:
            select_columns.extend(PRICE_FRAME_KEY_COLUMNS)
        if symbols is not None and len(symbols) == 0:
            return pd.DataFrame(columns=select_columns)

        conditions, params = self._build_price_conditions(
            symbols=symbols,
            timeframe=timeframe,
            exchange=exchange,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        where = " AND ".join(conditions) if conditions else "1"

        conn = self._connect()
        try:
            self._raise_for_ambiguous_read(conn, where, params, exchange, timeframe)
            sql = f"""
                SELECT {", ".join(select_columns)}
                FROM price_data
                WHERE {where}
                ORDER BY symbol, milliseconds
            """
            return pd.read_sql_query(sql, conn, params=params)
        finally:
            conn.close()

    def create_backtesting_view(
        self,
        view_name: str = "backtesting_price_data",
        exchange: str = "binance",
        timeframe: str = "1h",
    ) -> None:
        """Create or replace a legacy-compatible price view for one key."""
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", view_name):
            raise ValueError("view_name must be a simple SQLite identifier")

        conn = self._connect()
        try:
            exchange_literal = conn.execute("SELECT quote(?)", (exchange,)).fetchone()[0]
            timeframe_literal = conn.execute("SELECT quote(?)", (timeframe,)).fetchone()[0]
            conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            conn.execute(
                f"""
                CREATE VIEW {view_name} AS
                SELECT milliseconds, timestamp, symbol, price, volume
                FROM price_data
                WHERE exchange = {exchange_literal}
                  AND timeframe = {timeframe_literal}
                """
            )
            conn.commit()
        finally:
            conn.close()

    def _build_price_conditions(
        self,
        symbols: Optional[List[str]] = None,
        timeframe: Optional[str] = None,
        exchange: Optional[str] = None,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> Tuple[List[str], List[object]]:
        conditions: List[str] = []
        params: List[object] = []

        if exchange is not None:
            conditions.append("exchange = ?")
            params.append(exchange)
        if symbols:
            placeholders = ", ".join("?" for _ in symbols)
            conditions.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        if timeframe is not None:
            conditions.append("timeframe = ?")
            params.append(timeframe)
        if start_ms is not None:
            conditions.append("milliseconds >= ?")
            params.append(start_ms)
        if end_ms is not None:
            conditions.append("milliseconds <= ?")
            params.append(end_ms)

        return conditions, params

    def _raise_for_ambiguous_read(
        self,
        conn: sqlite3.Connection,
        where: str,
        params: List[object],
        exchange: Optional[str],
        timeframe: Optional[str],
    ) -> None:
        if exchange is None:
            row = conn.execute(
                f"SELECT COUNT(DISTINCT exchange) AS count FROM price_data WHERE {where}",
                params,
            ).fetchone()
            if row["count"] > 1:
                raise ValueError(
                    "exchange is required because multiple exchanges are present"
                )

        if timeframe is None:
            row = conn.execute(
                f"SELECT COUNT(DISTINCT timeframe) AS count FROM price_data WHERE {where}",
                params,
            ).fetchone()
            if row["count"] > 1:
                raise ValueError(
                    "timeframe is required because multiple timeframes are present"
                )

    def get_inventory(
        self,
        exchange: Optional[str] = None,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[InventoryRow]:
        """Return summary of stored data grouped by exchange/symbol/timeframe."""
        conditions = []
        params: List[str] = []
        if exchange:
            conditions.append("exchange = ?")
            params.append(exchange)
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if timeframe:
            conditions.append("timeframe = ?")
            params.append(timeframe)

        where = " AND ".join(conditions) if conditions else "1"
        sql = f"""
            SELECT
                exchange,
                symbol,
                timeframe,
                COUNT(*) AS rows,
                MIN(timestamp) AS first_dt,
                MAX(timestamp) AS last_dt
            FROM price_data
            WHERE {where}
            GROUP BY exchange, symbol, timeframe
            ORDER BY exchange, symbol, timeframe
        """

        conn = self._connect()
        try:
            rows = conn.execute(sql, params).fetchall()
            results = []
            for r in rows:
                results.append(
                    InventoryRow(
                        exchange=r["exchange"],
                        symbol=r["symbol"],
                        timeframe=r["timeframe"],
                        rows=r["rows"],
                        first_datetime_utc=r["first_dt"],
                        last_datetime_utc=r["last_dt"],
                    )
                )
            return results
        finally:
            conn.close()

    def validate(
        self,
        exchange: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[ValidationResult]:
        """Validate data integrity and detect gaps.

        Checks for:
        - Duplicate rows
        - Null values in critical columns (price, volume)
        - Non-positive price or volume
        - Missing bars (gaps) based on expected interval
        """
        inventory = self.get_inventory(exchange=exchange, timeframe=timeframe)
        results: List[ValidationResult] = []

        conn = self._connect()
        try:
            for inv in inventory:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN milliseconds IS NULL THEN 1 ELSE 0 END) AS null_ms,
                        SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_ts,
                        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) AS null_open,
                        SUM(CASE WHEN high IS NULL THEN 1 ELSE 0 END) AS null_high,
                        SUM(CASE WHEN low IS NULL THEN 1 ELSE 0 END) AS null_low,
                        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_price,
                        SUM(CASE WHEN volume IS NULL THEN 1 ELSE 0 END) AS null_vol,
                        SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END) AS bad_price,
                        SUM(CASE WHEN volume <= 0 THEN 1 ELSE 0 END) AS bad_vol
                    FROM price_data
                    WHERE exchange = ? AND symbol = ? AND timeframe = ?
                    """,
                    (inv.exchange, inv.symbol, inv.timeframe),
                ).fetchone()

                total = row["total"]
                null_count = int(
                    (row["null_ms"] or 0)
                    + (row["null_ts"] or 0)
                    + (row["null_open"] or 0)
                    + (row["null_high"] or 0)
                    + (row["null_low"] or 0)
                    + (row["null_price"] or 0)
                    + (row["null_vol"] or 0)
                )
                non_positive_price = int(row["bad_price"] or 0)
                non_positive_volume = int(row["bad_vol"] or 0)

                # Detect duplicates via GROUP BY having count > 1
                dup_row = conn.execute(
                    """
                    SELECT COUNT(*) AS dup_count FROM (
                        SELECT milliseconds
                        FROM price_data
                        WHERE exchange = ? AND symbol = ? AND timeframe = ?
                        GROUP BY exchange, symbol, timeframe, milliseconds
                        HAVING COUNT(*) > 1
                    )
                    """,
                    (inv.exchange, inv.symbol, inv.timeframe),
                ).fetchone()
                duplicate_count = int(dup_row["dup_count"] if dup_row else 0)

                # Detect gaps by iterating ordered timestamps
                expected_interval = self._guess_interval_ms(inv.timeframe)
                gap_count = 0
                largest_gap_bars = 0

                timestamps = conn.execute(
                    """
                    SELECT milliseconds FROM price_data
                    WHERE exchange = ? AND symbol = ? AND timeframe = ?
                    ORDER BY milliseconds ASC
                    """,
                    (inv.exchange, inv.symbol, inv.timeframe),
                ).fetchall()

                if len(timestamps) > 1:
                    prev = timestamps[0]["milliseconds"]
                    for ts_row in timestamps[1:]:
                        curr = ts_row["milliseconds"]
                        diff = curr - prev
                        if diff > expected_interval:
                            gap_bars = round(diff / expected_interval) - 1
                            gap_count += gap_bars
                            if gap_bars > largest_gap_bars:
                                largest_gap_bars = gap_bars
                        prev = curr

                status = "PASS"
                if duplicate_count > 0 or null_count > 0 or gap_count > 0:
                    status = "WARN"
                if non_positive_price > 0 or non_positive_volume > 0:
                    status = "FAIL"

                results.append(
                    ValidationResult(
                        exchange=inv.exchange,
                        symbol=inv.symbol,
                        timeframe=inv.timeframe,
                        rows=total,
                        duplicate_count=duplicate_count,
                        null_count=null_count,
                        non_positive_price_count=non_positive_price,
                        non_positive_volume_count=non_positive_volume,
                        expected_interval_ms=expected_interval,
                        gap_count=gap_count,
                        largest_gap_bars=largest_gap_bars,
                        first_datetime_utc=inv.first_datetime_utc,
                        last_datetime_utc=inv.last_datetime_utc,
                        status=status,
                    )
                )
            return results
        finally:
            conn.close()

    @staticmethod
    def _guess_interval_ms(timeframe: str) -> int:
        """Guess the expected interval in milliseconds for a timeframe string."""
        mapping = {
            "1m": 60_000,
            "5m": 300_000,
            "15m": 900_000,
            "30m": 1_800_000,
            "1h": 3_600_000,
            "2h": 7_200_000,
            "4h": 14_400_000,
            "6h": 21_600_000,
            "12h": 43_200_000,
            "1d": 86_400_000,
            "1w": 604_800_000,
            "1M": 2_592_000_000,
        }
        return mapping.get(timeframe, 3_600_000)
