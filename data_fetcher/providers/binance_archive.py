"""Binance public data archive ingestion."""

from __future__ import annotations

import csv
import logging
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve

from data_fetcher.storage.sqlite import SQLiteStore

logger = logging.getLogger(__name__)

BINANCE_ARCHIVE_BASE_URL = "https://data.binance.vision/data/spot"
BINANCE_SPOT_ARCHIVE_START = date(2017, 1, 1)


@dataclass
class ArchiveIngestResult:
    """Summary of a Binance archive ingestion run."""

    symbol: str
    files_downloaded: int = 0
    files_missing: int = 0
    files_cached: int = 0
    candles_seen: int = 0
    rows_inserted: int = 0


def binance_archive_symbol(symbol: str) -> str:
    """Convert a CCXT spot symbol like BTC/USDT to a Binance archive symbol."""
    return symbol.replace("/", "").replace(":", "").upper()


def parse_date_bound(value: str, default: date) -> date:
    """Parse 'YYYY-MM-DD' style CLI bounds."""
    if value in {"earliest", "start"}:
        return default
    if value in {"now", "today"}:
        return datetime.now(timezone.utc).date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _month_starts(start: date, end: date) -> Iterator[date]:
    current = date(start.year, start.month, 1)
    final = date(end.year, end.month, 1)
    while current <= final:
        yield current
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


def _days(start: date, end: date) -> Iterator[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def archive_filename(symbol_id: str, timeframe: str, period: date, granularity: str) -> str:
    """Return the expected Binance archive ZIP filename."""
    if granularity == "monthly":
        suffix = period.strftime("%Y-%m")
    elif granularity == "daily":
        suffix = period.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Unsupported archive granularity: {granularity}")
    return f"{symbol_id}-{timeframe}-{suffix}.zip"


def archive_url(symbol_id: str, timeframe: str, period: date, granularity: str) -> str:
    """Return the public Binance archive URL for a ZIP file."""
    filename = archive_filename(symbol_id, timeframe, period, granularity)
    return (
        f"{BINANCE_ARCHIVE_BASE_URL}/{granularity}/klines/"
        f"{symbol_id}/{timeframe}/{filename}"
    )


def local_archive_path(
    cache_dir: Path,
    symbol_id: str,
    timeframe: str,
    period: date,
    granularity: str,
) -> Path:
    """Return the local cache path for an archive ZIP."""
    return (
        cache_dir
        / "binance"
        / "spot"
        / granularity
        / "klines"
        / symbol_id
        / timeframe
        / archive_filename(symbol_id, timeframe, period, granularity)
    )


def _download_archive(url: str, destination: Path) -> str:
    """Download an archive file if available.

    Returns one of: downloaded, cached, missing.
    """
    if destination.exists() and destination.stat().st_size > 0:
        return "cached"

    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        urlretrieve(url, destination)
        if destination.stat().st_size == 0:
            destination.unlink(missing_ok=True)
            return "missing"
        return "downloaded"
    except HTTPError as exc:
        destination.unlink(missing_ok=True)
        if exc.code == 404:
            return "missing"
        raise
    except URLError:
        destination.unlink(missing_ok=True)
        raise


def _iter_zip_klines(path: Path) -> Iterator[Tuple[int, float, float, float, float, float]]:
    """Yield OHLCV tuples from a Binance kline ZIP."""
    with zipfile.ZipFile(path) as zf:
        names = [name for name in zf.namelist() if not name.endswith("/")]
        if not names:
            return
        with zf.open(names[0]) as raw:
            reader = csv.reader(line.decode("utf-8") for line in raw)
            for row in reader:
                if len(row) < 6:
                    continue
                try:
                    yield (
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                    )
                except ValueError:
                    continue


def _iso8601_from_ms(milliseconds: int) -> str:
    return (
        datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _periods_for_range(
    start: date,
    end: date,
    include_daily_current_month: bool,
) -> List[Tuple[str, date]]:
    today = datetime.now(timezone.utc).date()
    current_month = date(today.year, today.month, 1)
    periods: List[Tuple[str, date]] = []

    for month in _month_starts(start, end):
        if include_daily_current_month and month >= current_month:
            continue
        periods.append(("monthly", month))

    if include_daily_current_month and end >= current_month:
        daily_start = max(start, current_month)
        daily_end = min(end, today)
        periods.extend(("daily", day) for day in _days(daily_start, daily_end))

    return periods


def ingest_binance_archives(
    *,
    store: SQLiteStore,
    symbol: str,
    timeframe: str,
    since: date,
    until: date,
    cache_dir: Path,
    resume: bool = True,
    include_daily_current_month: bool = True,
    chunk_size: int = 10_000,
) -> ArchiveIngestResult:
    """Download Binance archive ZIPs and insert their candles into SQLite."""
    result = ArchiveIngestResult(symbol=symbol)
    symbol_id = binance_archive_symbol(symbol)

    local_max_ms: Optional[int] = None
    if resume:
        local_max_ms = store.get_max_timestamp("binance", symbol, timeframe)

    since_ms = int(datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    until_ms = int(datetime.combine(until, datetime.max.time(), tzinfo=timezone.utc).timestamp() * 1000)
    if local_max_ms is not None:
        since_ms = max(since_ms, local_max_ms + 1)

    rows: List[Tuple] = []
    for granularity, period in _periods_for_range(since, until, include_daily_current_month):
        path = local_archive_path(cache_dir, symbol_id, timeframe, period, granularity)
        status = _download_archive(
            archive_url(symbol_id, timeframe, period, granularity),
            path,
        )
        if status == "missing":
            result.files_missing += 1
            continue
        if status == "downloaded":
            result.files_downloaded += 1
        elif status == "cached":
            result.files_cached += 1

        for ms, open_, high, low, close, volume in _iter_zip_klines(path):
            if ms < since_ms or ms > until_ms:
                continue
            result.candles_seen += 1
            rows.append(
                (
                    ms,
                    _iso8601_from_ms(ms),
                    "binance",
                    symbol,
                    timeframe,
                    open_,
                    high,
                    low,
                    close,
                    volume,
                )
            )
            if len(rows) >= chunk_size:
                result.rows_inserted += store.insert_ohlcv(rows)
                rows = []

    if rows:
        result.rows_inserted += store.insert_ohlcv(rows)

    return result
