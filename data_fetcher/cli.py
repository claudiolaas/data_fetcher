"""CLI entrypoint for the data-fetcher package."""

import logging
import sys
import time as time_module
from pathlib import Path
from typing import List, Optional

import typer

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
) -> None:
    """List available symbols from an exchange."""
    try:
        fetcher = CryptoDataFetcher(exchange_id=exchange)
        results = fetcher.get_symbols(quote=quote, active_only=active_only, spot_only=spot_only)
    except Exception as e:
        typer.echo(f"Error loading exchange {exchange}: {e}", err=True)
        raise typer.Exit(code=1)

    if base:
        results = [r for r in results if r.get("base") == base]
    if contains:
        results = [r for r in results if contains.upper() in r["symbol"].upper()]

    if limit > 0:
        results = results[:limit]

    if not results:
        typer.echo("No symbols found matching the given filters.")
        raise typer.Exit(code=0)

    header = f"{'symbol':<20} {'base':<12} {'quote':<12} {'type':<8} {'active':<8}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['symbol']:<20} {str(r['base']):<12} {str(r['quote']):<12} "
            f"{r['type']:<8} {str(r['active']):<8}"
        )


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


def main() -> None:
    """Legacy entry point for setup.py console_scripts compatibility."""
    app()


if __name__ == "__main__":
    app()
