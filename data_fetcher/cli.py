"""CLI entrypoint for the data-fetcher package."""

import logging
import sys
import time as time_module
from pathlib import Path
from typing import Dict, List, Optional

import typer

from data_fetcher.providers.binance_archive import (
    BINANCE_SPOT_ARCHIVE_START,
    ingest_binance_archives,
    parse_date_bound,
)
from data_fetcher.providers.crypto import (
    CryptoDataFetcher,
    DEFAULT_EXCHANGE,
)
from data_fetcher.storage.sqlite import SQLiteStore

logger = logging.getLogger(__name__)

app = typer.Typer(
    name="data-fetcher",
    help="Fetch and manage historical OHLCV data for crypto and other markets.",
)
crypto_app = typer.Typer(help="Crypto OHLCV commands backed by CCXT.")
alpaca_app = typer.Typer(help="Alpaca market data commands.")

EXCHANGE_LABELS = {
    "binance": "Binance",
    "coinbase": "Coinbase",
    "kraken": "Kraken",
    "okx": "OKX",
    "bybit": "Bybit",
}


def _setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )


def _read_symbol_file(symbols_file: Path) -> List[str]:
    symbol_list: List[str] = []
    with open(symbols_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                symbol_list.append(line)
    return symbol_list


def _filter_discovered_symbols(
    rows: List[Dict],
    base: Optional[str] = None,
    contains: Optional[str] = None,
    limit: int = 200,
) -> List[Dict]:
    if base:
        rows = [r for r in rows if r.get("base") == base]
    if contains:
        rows = [r for r in rows if contains.upper() in r["symbol"].upper()]
    if limit > 0:
        rows = rows[:limit]
    return rows


def _build_symbol_rows(
    fetcher: CryptoDataFetcher,
    symbols: Optional[str],
    symbols_file: Optional[Path],
    quote: Optional[str],
    active_only: bool,
    spot_only: bool,
    base: Optional[str],
    contains: Optional[str],
    limit: int,
) -> List[Dict]:
    symbol_list: List[str] = []
    if symbols:
        symbol_list.extend(s.strip() for s in symbols.split(",") if s.strip())
    if symbols_file:
        symbol_list.extend(_read_symbol_file(symbols_file))

    if symbol_list:
        markets = fetcher.get_markets()
        rows = []
        for symbol in symbol_list:
            info = markets.get(symbol, {})
            rows.append(
                {
                    "symbol": symbol,
                    "base": info.get("base", ""),
                    "quote": info.get("quote", ""),
                    "type": info.get("type", ""),
                    "active": info.get("active", ""),
                }
            )
        return rows

    discovered = fetcher.get_symbols(
        quote=quote,
        active_only=active_only,
        spot_only=spot_only,
    )
    return _filter_discovered_symbols(
        discovered,
        base=base,
        contains=contains,
        limit=limit,
    )


@app.callback()
def callback(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
) -> None:
    """Configure global CLI options."""
    _setup_logging(verbose)


@app.command()
def exchanges() -> None:
    """List supported crypto exchanges and highlight the default."""
    header = f"{'exchange':<20} {'has_fetch_ohlcv':<18} {'rate_limit_ms':<15} {'default':<10}"
    print(header)
    print("-" * len(header))

    for eid, label in EXCHANGE_LABELS.items():
        try:
            exchange_class = __import__("ccxt", fromlist=[eid])
            ex = getattr(exchange_class, eid)()
            has_ohlcv = hasattr(ex, "fetch_ohlcv") and callable(ex.fetch_ohlcv)
            rate_limit = getattr(ex, "rateLimit", "N/A")
            is_default = "yes" if eid == DEFAULT_EXCHANGE else ""
            print(
                f"{label:<20} {str(has_ohlcv):<18} {str(rate_limit):<15} {is_default:<10}"
            )
        except Exception:
            print(f"{label:<20} {'error':<18} {'N/A':<15} {'':<10}")


@app.command()
def symbols(
    exchange: str = typer.Option(DEFAULT_EXCHANGE, "--exchange", "-e", help="CCXT exchange ID"),
    quote: Optional[str] = typer.Option(None, "--quote", "-q", help="Filter by quote currency (e.g. USDT)"),
    active_only: bool = typer.Option(True, "--active-only/--include-inactive", help="Only show active markets"),
    spot_only: bool = typer.Option(True, "--spot-only/--all-types", help="Only show spot markets"),
    base: Optional[str] = typer.Option(None, "--base", "-b", help="Filter by base currency"),
    contains: Optional[str] = typer.Option(None, "--contains", help="Filter symbol containing text"),
    limit: int = typer.Option(200, "--limit", "-n", help="Maximum number of symbols to show"),
    output_format: str = typer.Option(
        "table",
        "--format",
        help="Output format: table or symbols",
    ),
) -> None:
    """List available symbols from an exchange."""
    try:
        fetcher = CryptoDataFetcher(exchange_id=exchange)
        results = fetcher.get_symbols(
            quote=quote,
            active_only=active_only,
            spot_only=spot_only,
        )
    except Exception as e:
        typer.echo(f"Error loading exchange {exchange}: {e}", err=True)
        raise typer.Exit(code=1)

    results = _filter_discovered_symbols(
        results,
        base=base,
        contains=contains,
        limit=limit,
    )

    if not results:
        typer.echo("No symbols found matching the given filters.")
        raise typer.Exit(code=0)

    if output_format == "symbols":
        for r in results:
            print(r["symbol"])
        return
    if output_format != "table":
        typer.echo("Invalid --format. Use 'table' or 'symbols'.", err=True)
        raise typer.Exit(code=1)

    header = f"{'symbol':<20} {'base':<12} {'quote':<12} {'type':<8} {'active':<8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['symbol']:<20} {str(r['base']):<12} {str(r['quote']):<12} "
            f"{r['type']:<8} {str(r['active']):<8}"
        )


@app.command()
def start_dates(
    exchange: str = typer.Option(
        DEFAULT_EXCHANGE,
        "--exchange",
        "-e",
        help="CCXT exchange ID",
    ),
    symbols: Optional[str] = typer.Option(
        None,
        "--symbols",
        "-s",
        help="Comma-separated symbol list",
    ),
    symbols_file: Optional[Path] = typer.Option(
        None,
        "--symbols-file",
        "-f",
        help="File with one symbol per line",
    ),
    quote: Optional[str] = typer.Option(
        None,
        "--quote",
        "-q",
        help="Filter by quote currency when discovering symbols",
    ),
    timeframe: str = typer.Option(
        "1h",
        "--timeframe",
        "-t",
        help="Candle timeframe used for earliest-candle probing",
    ),
    active_only: bool = typer.Option(
        True,
        "--active-only/--include-inactive",
        help="Only discover active markets",
    ),
    spot_only: bool = typer.Option(
        True,
        "--spot-only/--all-types",
        help="Only discover spot markets",
    ),
    base: Optional[str] = typer.Option(
        None,
        "--base",
        "-b",
        help="Filter discovered symbols by base currency",
    ),
    contains: Optional[str] = typer.Option(
        None,
        "--contains",
        help="Filter discovered symbols containing text",
    ),
    limit: int = typer.Option(
        200,
        "--limit",
        "-n",
        help="Maximum number of symbols to check when discovering (0 = unlimited)",
    ),
) -> None:
    """Show earliest available OHLCV start dates for symbols."""
    try:
        fetcher = CryptoDataFetcher(exchange_id=exchange)
    except Exception as e:
        typer.echo(f"Error loading exchange {exchange}: {e}", err=True)
        raise typer.Exit(code=1)

    symbol_rows = _build_symbol_rows(
        fetcher=fetcher,
        symbols=symbols,
        symbols_file=symbols_file,
        quote=quote,
        active_only=active_only,
        spot_only=spot_only,
        base=base,
        contains=contains,
        limit=limit,
    )
    symbol_list = [r["symbol"] for r in symbol_rows]

    if not symbol_list:
        typer.echo("No symbols to check.")
        raise typer.Exit(code=0)

    header = f"{'exchange':<12} {'symbol':<20} {'timeframe':<10} {'start_date_utc':<25} {'method':<14}"
    print(header)
    print("-" * len(header))

    for sym in symbol_list:
        ts, method = fetcher.fetch_earliest_timestamp(sym, timeframe)
        start = fetcher.exchange.iso8601(ts) if ts is not None else "N/A"
        print(f"{exchange:<12} {sym:<20} {timeframe:<10} {start:<25} {method:<14}")


@app.command()
def fetch(
    exchange: str = typer.Option(DEFAULT_EXCHANGE, "--exchange", "-e", help="CCXT exchange ID"),
    symbols: Optional[str] = typer.Option(None, "--symbols", "-s", help="Comma-separated symbol list"),
    symbols_file: Optional[Path] = typer.Option(None, "--symbols-file", "-f", help="File with one symbol per line"),
    quote: Optional[str] = typer.Option(None, "--quote", "-q", help="Filter by quote currency (auto-discover)"),
    timeframe: str = typer.Option("1h", "--timeframe", "-t", help="Candle timeframe"),
    since: str = typer.Option("earliest", "--since", help="Start: 'earliest', 'YYYY-MM-DD', or milliseconds"),
    until: str = typer.Option("now", "--until", help="End: 'now', 'YYYY-MM-DD', or milliseconds"),
    db_path: str = typer.Option("data/crypto_ohlcv.db", "--db-path", "-d", help="Path to SQLite database"),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume from last stored timestamp"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Delete existing data before fetching"),
    limit_symbols: int = typer.Option(0, "--limit-symbols", "-n", help="Max symbols to fetch (0 = unlimited)"),
    max_requests_per_symbol: Optional[int] = typer.Option(None, "--max-requests-per-symbol", help="Max API requests per symbol"),
    sleep_seconds: float = typer.Option(0.12, "--sleep-seconds", help="Seconds to wait between API requests"),
    workers: int = typer.Option(1, "--workers", help="Number of concurrent workers (reserved for future use)"),
    fail_fast: bool = typer.Option(False, "--fail-fast", help="Stop on first error (reserved for future use)"),
) -> None:
    """Fetch OHLCV data for one or more symbols into a SQLite database."""
    store = SQLiteStore(db_path)

    symbol_list: List[str] = []
    if symbols:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if symbols_file:
        with open(symbols_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    symbol_list.append(line)

    if not symbol_list:
        typer.echo("No symbols specified, discovering from exchange...")
        fetcher = CryptoDataFetcher(exchange_id=exchange)
        discovered = fetcher.get_symbols(quote=quote, active_only=True, spot_only=True)
        symbol_list = [r["symbol"] for r in discovered]
        if limit_symbols > 0:
            symbol_list = symbol_list[:limit_symbols]

    if not symbol_list:
        typer.echo("No symbols to fetch.")
        raise typer.Exit(code=0)

    fetcher = CryptoDataFetcher(exchange_id=exchange)
    now_ms = int(time_module.time() * 1000)

    since_ms: Optional[int] = None
    if since == "earliest":
        since_ms = None
    elif since.isdigit():
        since_ms = int(since)
    else:
        parsed = fetcher.exchange.parse8601(f"{since}T00:00:00Z")
        since_ms = int(parsed) if parsed is not None else None

    until_ms: Optional[int] = None
    if until == "now":
        until_ms = now_ms
    elif until.isdigit():
        until_ms = int(until)
    else:
        parsed = fetcher.exchange.parse8601(f"{until}T00:00:00Z")
        until_ms = int(parsed) if parsed is not None else None

    typer.echo(f"Fetching {len(symbol_list)} symbols on {exchange} [{timeframe}]")

    for i, sym in enumerate(symbol_list):
        typer.echo(f"\n[{i+1}/{len(symbol_list)}] {sym}")

        local_max_ms = store.get_max_timestamp(exchange, sym, timeframe)

        if overwrite and local_max_ms is not None:
            deleted = store.delete_for_key(exchange, sym, timeframe)
            typer.echo(f"  Deleted {deleted} existing rows (--overwrite)")
            local_max_ms = None

        effective_since = since_ms
        if resume and local_max_ms is not None:
            effective_since = local_max_ms + 1
            typer.echo(f"  Resuming from timestamp {effective_since}")

        if effective_since is None:
            earliest_ts, method = fetcher.fetch_earliest_timestamp(sym, timeframe)
            if earliest_ts is not None:
                effective_since = earliest_ts
                typer.echo(f"  Earliest data: {earliest_ts} ({method})")
            else:
                typer.echo("  Could not determine earliest timestamp, skipping.")
                continue

        candles = fetcher.fetch_ohlcv(
            symbol=sym,
            timeframe=timeframe,
            since=effective_since,
            until=until_ms,
            max_requests=max_requests_per_symbol,
            sleep_seconds=sleep_seconds,
        )

        if not candles:
            typer.echo("  No data returned.")
            continue

        rows = []
        for c in candles:
            ms = int(c[0])
            ts_iso = fetcher.exchange.iso8601(ms)
            rows.append((
                ms,
                ts_iso,
                exchange,
                sym,
                timeframe,
                float(c[1]),
                float(c[2]),
                float(c[3]),
                float(c[4]),
                float(c[5]),
            ))

        inserted = store.insert_ohlcv(rows)
        typer.echo(f"  Inserted {inserted} rows ({len(candles)} candles fetched)")

        # Sleep between symbols (per-request pacing is handled inside fetch_ohlcv)
        if sleep_seconds > 0 and i < len(symbol_list) - 1:
            time_module.sleep(sleep_seconds)

    typer.echo("\nDone.")


@app.command()
def bulk_fetch(
    symbols: Optional[str] = typer.Option(None, "--symbols", "-s", help="Comma-separated symbol list"),
    symbols_file: Optional[Path] = typer.Option(None, "--symbols-file", "-f", help="File with one symbol per line"),
    quote: Optional[str] = typer.Option(None, "--quote", "-q", help="Filter by quote currency (auto-discover)"),
    timeframe: str = typer.Option("1h", "--timeframe", "-t", help="Candle timeframe"),
    since: str = typer.Option("earliest", "--since", help="Start: 'earliest', 'YYYY-MM-DD'"),
    until: str = typer.Option("now", "--until", help="End: 'now' or 'YYYY-MM-DD'"),
    db_path: str = typer.Option("data/crypto_ohlcv.db", "--db-path", "-d", help="Path to SQLite database"),
    cache_dir: Path = typer.Option("data/archive_cache", "--cache-dir", help="Directory for downloaded archive ZIPs"),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Skip rows already present locally"),
    limit_symbols: int = typer.Option(0, "--limit-symbols", "-n", help="Max symbols to fetch (0 = unlimited)"),
    include_daily_current_month: bool = typer.Option(
        True,
        "--include-daily-current-month/--monthly-only",
        help="Use daily ZIPs to fill the current month after monthly archive files",
    ),
) -> None:
    """Bulk ingest Binance public archive OHLCV ZIPs into SQLite."""
    symbol_list: List[str] = []
    if symbols:
        symbol_list.extend(s.strip() for s in symbols.split(",") if s.strip())
    if symbols_file:
        symbol_list.extend(_read_symbol_file(symbols_file))

    if not symbol_list:
        typer.echo("No symbols specified, discovering active Binance spot symbols...")
        fetcher = CryptoDataFetcher(exchange_id="binance")
        discovered = fetcher.get_symbols(quote=quote, active_only=True, spot_only=True)
        symbol_list = [r["symbol"] for r in discovered]

    if limit_symbols > 0:
        symbol_list = symbol_list[:limit_symbols]

    if not symbol_list:
        typer.echo("No symbols to fetch.")
        raise typer.Exit(code=0)

    try:
        since_date = parse_date_bound(since, BINANCE_SPOT_ARCHIVE_START)
        until_date = parse_date_bound(until, datetime_now_utc_date())
    except ValueError as exc:
        typer.echo(f"Invalid date bound: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if until_date < since_date:
        typer.echo("--until must be on or after --since", err=True)
        raise typer.Exit(code=1)

    store = SQLiteStore(db_path)

    if timeframe == "1M":
        _bulk_fetch_sparse_timeframe(
            store=store,
            symbol_list=symbol_list,
            timeframe=timeframe,
            since=since,
            until=until,
            resume=resume,
        )
        return

    typer.echo(
        f"Bulk fetching {len(symbol_list)} Binance symbols [{timeframe}] "
        f"from {since_date} to {until_date}"
    )
    typer.echo(f"Archive cache: {Path(cache_dir).expanduser()}")

    total_inserted = 0
    total_seen = 0
    for i, sym in enumerate(symbol_list):
        typer.echo(f"\n[{i+1}/{len(symbol_list)}] {sym}")
        try:
            result = ingest_binance_archives(
                store=store,
                symbol=sym,
                timeframe=timeframe,
                since=since_date,
                until=until_date,
                cache_dir=Path(cache_dir).expanduser(),
                resume=resume,
                include_daily_current_month=include_daily_current_month,
            )
        except Exception as exc:
            typer.echo(f"  Error: {exc}", err=True)
            continue

        total_inserted += result.rows_inserted
        total_seen += result.candles_seen
        typer.echo(
            "  "
            f"Inserted {result.rows_inserted} rows "
            f"({result.candles_seen} candles, "
            f"{result.files_downloaded} downloaded, "
            f"{result.files_cached} cached, "
            f"{result.files_missing} missing)"
        )

    typer.echo(f"\nDone. Inserted {total_inserted} rows from {total_seen} candles.")


def _parse_exchange_bound(fetcher: CryptoDataFetcher, value: str, now_ms: int) -> Optional[int]:
    if value == "earliest":
        return None
    if value == "now":
        return now_ms
    if value.isdigit():
        return int(value)
    parsed = fetcher.exchange.parse8601(f"{value}T00:00:00Z")
    return int(parsed) if parsed is not None else None


def _bulk_fetch_sparse_timeframe(
    *,
    store: SQLiteStore,
    symbol_list: List[str],
    timeframe: str,
    since: str,
    until: str,
    resume: bool,
) -> None:
    """Fetch sparse timeframes through CCXT instead of archive ZIPs.

    Binance archive ZIPs are partitioned by calendar month. For a monthly
    candle interval, that turns into roughly one HTTP file per candle, while
    CCXT can usually retrieve the whole series in a single request.
    """
    fetcher = CryptoDataFetcher(exchange_id="binance")
    now_ms = int(time_module.time() * 1000)
    since_ms = _parse_exchange_bound(fetcher, since, now_ms)
    until_ms = _parse_exchange_bound(fetcher, until, now_ms)

    typer.echo(
        f"Bulk fetching {len(symbol_list)} Binance symbols [{timeframe}] "
        "through the exchange API because archive ZIPs are inefficient for "
        "monthly candles"
    )

    total_inserted = 0
    total_seen = 0
    for i, sym in enumerate(symbol_list):
        typer.echo(f"\n[{i+1}/{len(symbol_list)}] {sym}")

        effective_since = since_ms
        local_max_ms = store.get_max_timestamp("binance", sym, timeframe)
        if resume and local_max_ms is not None:
            effective_since = local_max_ms + 1
            typer.echo(f"  Resuming from timestamp {effective_since}")

        if effective_since is None:
            earliest_ts, method = fetcher.fetch_earliest_timestamp(sym, timeframe)
            if earliest_ts is None:
                typer.echo("  Could not determine earliest timestamp, skipping.")
                continue
            effective_since = earliest_ts
            typer.echo(f"  Earliest data: {earliest_ts} ({method})")

        candles = fetcher.fetch_ohlcv(
            symbol=sym,
            timeframe=timeframe,
            since=effective_since,
            until=until_ms,
            limit=1000,
            sleep_seconds=0.0,
        )
        if not candles:
            typer.echo("  No data returned.")
            continue

        rows = []
        for candle in candles:
            ms = int(candle[0])
            rows.append(
                (
                    ms,
                    fetcher.exchange.iso8601(ms),
                    "binance",
                    sym,
                    timeframe,
                    float(candle[1]),
                    float(candle[2]),
                    float(candle[3]),
                    float(candle[4]),
                    float(candle[5]),
                )
            )

        inserted = store.insert_ohlcv(rows)
        total_inserted += inserted
        total_seen += len(candles)
        typer.echo(f"  Inserted {inserted} rows ({len(candles)} candles fetched)")

    typer.echo(f"\nDone. Inserted {total_inserted} rows from {total_seen} candles.")


def datetime_now_utc_date():
    """Return today's UTC date.

    Small helper keeps Typer defaults static while allowing tests to patch the
    date parser independently from import time.
    """
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).date()


@app.command()
def inventory(
    db_path: str = typer.Option("data/crypto_ohlcv.db", "--db-path", "-d", help="Path to SQLite database"),
    exchange: Optional[str] = typer.Option(None, "--exchange", "-e", help="Filter by exchange"),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s", help="Filter by symbol"),
    timeframe: Optional[str] = typer.Option(None, "--timeframe", "-t", help="Filter by timeframe"),
) -> None:
    """Show stored data inventory from the SQLite database."""
    store = SQLiteStore(db_path)
    rows = store.get_inventory(exchange=exchange, symbol=symbol, timeframe=timeframe)

    if not rows:
        typer.echo("No data found in the database.")
        raise typer.Exit(code=0)

    header = f"{'exchange':<12} {'symbol':<20} {'timeframe':<10} {'rows':<10} {'first (UTC)':<25} {'last (UTC)':<25}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r.exchange:<12} {r.symbol:<20} {r.timeframe:<10} {r.rows:<10} "
            f"{str(r.first_datetime_utc):<25} {str(r.last_datetime_utc):<25}"
        )


@app.command()
def validate(
    db_path: str = typer.Option("data/crypto_ohlcv.db", "--db-path", "-d", help="Path to SQLite database"),
    exchange: Optional[str] = typer.Option(None, "--exchange", "-e", help="Filter by exchange"),
    timeframe: Optional[str] = typer.Option(None, "--timeframe", "-t", help="Filter by timeframe"),
) -> None:
    """Validate OHLCV data integrity in the SQLite database."""
    store = SQLiteStore(db_path)
    results = store.validate(exchange=exchange, timeframe=timeframe)

    if not results:
        typer.echo("No data to validate.")
        raise typer.Exit(code=0)

    header = (
        f"{'symbol':<20} {'rows':<8} {'dup':<6} {'null':<6} {'bad_price':<10} "
        f"{'bad_vol':<8} {'gap':<6} {'max_gap':<8} {'status':<8}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r.symbol:<20} {r.rows:<8} {r.duplicate_count:<6} {r.null_count:<6} "
            f"{r.non_positive_price_count:<10} {r.non_positive_volume_count:<8} "
            f"{r.gap_count:<6} {r.largest_gap_bars:<8} {r.status:<8}"
        )


@app.command("export-prices")
def export_prices(
    db_path: str = typer.Option("data/crypto_ohlcv.db", "--db-path", "-d", help="Path to SQLite database"),
    exchange: Optional[str] = typer.Option(None, "--exchange", "-e", help="Filter by exchange"),
    timeframe: Optional[str] = typer.Option(None, "--timeframe", "-t", help="Filter by timeframe"),
    symbols: Optional[str] = typer.Option(None, "--symbols", "-s", help="Comma-separated symbol list"),
    start_ms: Optional[int] = typer.Option(None, "--start-ms", help="Inclusive start timestamp in milliseconds"),
    end_ms: Optional[int] = typer.Option(None, "--end-ms", help="Inclusive end timestamp in milliseconds"),
    output_format: str = typer.Option("csv", "--format", help="Output format: csv or json"),
) -> None:
    """Export backtesting-compatible price rows from SQLite."""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None
    store = SQLiteStore(db_path)

    try:
        df = store.load_prices(
            symbols=symbol_list,
            exchange=exchange,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
        )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    if output_format == "csv":
        typer.echo(df.to_csv(index=False), nl=False)
    elif output_format == "json":
        typer.echo(df.to_json(orient="records"))
    else:
        typer.echo("Invalid --format. Use 'csv' or 'json'.", err=True)
        raise typer.Exit(code=1)


@alpaca_app.command("symbols")
def alpaca_symbols(
    contains: Optional[str] = typer.Option(None, "--contains", help="Filter symbol containing text"),
    limit: int = typer.Option(200, "--limit", "-n", help="Maximum number of symbols to show, 0 for all"),
) -> None:
    """List Alpaca asset symbols.

    Requires the optional Alpaca dependencies and ALPACA_API_KEY /
    ALPACA_SECRET_KEY credentials.
    """
    try:
        from data_fetcher.data import AlpacaDataFetcher
    except ImportError as e:
        typer.echo(
            "Alpaca dependencies are not installed. Install with: "
            'uv pip install -e ".[alpaca]"',
            err=True,
        )
        raise typer.Exit(code=1) from e

    try:
        fetcher = AlpacaDataFetcher()
        results = list(fetcher.get_ticker())
    except Exception as e:
        typer.echo(f"Error loading Alpaca symbols: {e}", err=True)
        raise typer.Exit(code=1) from e

    if contains:
        results = [symbol for symbol in results if contains.upper() in symbol.upper()]
    results = sorted(results)
    if limit > 0:
        results = results[:limit]

    if not results:
        typer.echo("No Alpaca symbols found matching the given filters.")
        raise typer.Exit(code=0)

    print(f"{'symbol':<20}")
    print("-" * 20)
    for symbol in results:
        print(f"{symbol:<20}")


# Provider-scoped aliases. The root commands remain for backwards
# compatibility with the initial crypto-only CLI.
crypto_app.command("exchanges")(exchanges)
crypto_app.command("symbols")(symbols)
crypto_app.command("start-dates")(start_dates)
crypto_app.command("fetch")(fetch)
crypto_app.command("bulk-fetch")(bulk_fetch)
crypto_app.command("inventory")(inventory)
crypto_app.command("validate")(validate)
crypto_app.command("export-prices")(export_prices)
app.add_typer(crypto_app, name="crypto")
app.add_typer(alpaca_app, name="alpaca")


def main() -> None:
    """Legacy entry point for setup.py console_scripts compatibility."""
    app()


if __name__ == "__main__":
    app()
