# data-fetcher

Reusable market-data fetching utilities for research projects.

The primary supported workflow is a crypto OHLCV command-line tool built on
`ccxt`. It discovers symbols, fetches historical candles, stores them in a
resumable SQLite database, and validates local data quality. Binance is the
default exchange, but any CCXT exchange id can be selected.

Alpaca and Polygon compatibility classes remain available for projects that
still use the older provider APIs.

## Installation

Requires Python 3.9 or newer.

From a local checkout:

```bash
git clone https://github.com/claudiolaas/data_fetcher.git
cd data_fetcher
uv venv
uv sync --extra crypto --extra dev
```

For optional provider dependencies:

```bash
uv sync --extra alpaca --extra polygon --extra dev
```

Use `uv sync --extra ...` when running commands through `uv run`; it installs
dependencies into this project's `.venv`. `uv pip install ...` can target an
already-active external virtualenv unless `--active` is used.

Useful extras:

```text
crypto   ccxt-based crypto fetching
alpaca   Alpaca compatibility provider
polygon  Polygon compatibility provider
all      all provider dependencies
dev      pytest, ruff, and local development tools
```

## Quick Start

Fetch one Binance spot market into SQLite, inspect the stored range, then run
data-quality checks:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db \
  --max-requests-per-symbol 1

uv run data-fetcher inventory --db-path data/crypto_ohlcv.db
uv run data-fetcher validate --db-path data/crypto_ohlcv.db
```

Most crypto commands make live exchange requests. Keep `--max-requests-per-symbol`
low for smoke tests, then remove it for full backfills.

## CLI

The package exposes one command:

```bash
uv run data-fetcher --help
```

Available commands:

```text
exchanges
symbols
start-dates
fetch
bulk-fetch
inventory
validate
crypto ...
alpaca symbols
```

The original top-level commands are crypto commands. Provider-scoped aliases are
also available:

```bash
uv run data-fetcher crypto symbols --exchange binance --quote USDT
uv run data-fetcher crypto start-dates --exchange binance --symbols BTC/USDT
uv run data-fetcher crypto fetch --exchange binance --symbols BTC/USDT
uv run data-fetcher crypto bulk-fetch --symbols BTC/USDT
uv run data-fetcher alpaca symbols
```

### List Exchanges

```bash
uv run data-fetcher exchanges
```

### List Symbols

List active Binance spot USDT symbols:

```bash
uv run data-fetcher symbols \
  --exchange binance \
  --quote USDT \
  --active-only
```

Useful filters:

```bash
uv run data-fetcher symbols --exchange binance --base BTC
uv run data-fetcher symbols --exchange binance --contains ETH
uv run data-fetcher symbols --exchange binance --quote USDT --limit 50
uv run data-fetcher symbols --exchange binance --quote USDT --format symbols
```

Provider-scoped equivalent:

```bash
uv run data-fetcher crypto symbols \
  --exchange binance \
  --quote USDC \
  --active-only \
  --limit 0
```

List Alpaca symbols:

```bash
uv run data-fetcher alpaca symbols --limit 0
```

Alpaca commands require optional dependencies and credentials:

```bash
uv sync --extra alpaca --extra dev
export ALPACA_API_KEY=...
export ALPACA_SECRET_KEY=...
uv run data-fetcher alpaca symbols --limit 0
```

### Start Dates

Show the first available OHLCV candle for explicit symbols:

```bash
uv run data-fetcher start-dates \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT \
  --timeframe 1h
```

Discover symbols with the same filters as `symbols`, then probe each start date:

```bash
uv run data-fetcher crypto start-dates \
  --exchange binance \
  --quote USDC \
  --active-only \
  --limit 0 \
  --timeframe 1h
```

For larger symbol sets, write a symbol list and pass it back in:

```bash
uv run data-fetcher symbols \
  --exchange binance \
  --quote USDC \
  --limit 0 \
  --format symbols > symbols.txt

uv run data-fetcher start-dates --exchange binance --symbols-file symbols.txt
```

### Visual Symbol Lifetime Report

The visual report is intentionally kept outside the main CLI. Use the standalone
script when you want an exploratory HTML view of which symbols have existed and
which are still active according to the exchange metadata.

```bash
uv run python scripts/symbol_lifetime_report.py \
  --exchange binance \
  --quote USDC \
  --limit 0 \
  --csv-output reports/binance_usdc_lifetimes.csv \
  --html-output reports/binance_usdc_lifetimes.html
```

Add `--active-only` to exclude inactive markets, or `--all-types` to include
non-spot markets exposed by CCXT.

### Fetch OHLCV

Fetch one symbol:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Fetch a small basket:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT,FET/USDT,CELR/USDT,THETA/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Fetch from a symbol file:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols-file symbols.txt \
  --timeframe 1h \
  --since 2020-01-01 \
  --until 2024-01-01 \
  --db-path data/crypto_ohlcv.db
```

Smoke-test a live fetch with only one exchange request:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path /tmp/crypto_ohlcv_smoke.db \
  --max-requests-per-symbol 1
```

Fetch behavior:

```text
--resume is enabled by default
--overwrite deletes existing rows for exchange/symbol/timeframe before fetching
--until is enforced, so candles after the requested bound are not returned
--sleep-seconds controls pagination pacing and symbol-to-symbol pacing
--workers and --fail-fast are reserved for future use
```

### Bulk Fetch Binance Archives

For large Binance backfills, use the public archive ZIP ingestion path. This is
much faster than paginating through the exchange API because it downloads
monthly candle files and inserts them directly into the same SQLite schema.

Discover active USDT symbols, then ingest their complete 1h archive history:

```bash
uv run data-fetcher symbols \
  --exchange binance \
  --quote USDT \
  --active-only \
  --limit 0 \
  --format symbols > symbols.txt

uv run data-fetcher bulk-fetch \
  --symbols-file symbols.txt \
  --timeframe 1h \
  --since earliest \
  --until now \
  --db-path data/crypto_ohlcv.db \
  --cache-dir data/archive_cache
```

You can also let the command discover Binance spot symbols by quote:

```bash
uv run data-fetcher bulk-fetch \
  --quote USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Bulk fetch behavior:

```text
only Binance spot public archives are supported
monthly ZIPs are used for completed months
daily ZIPs are used for the current month unless --monthly-only is passed
--timeframe 1M uses the exchange API because archive ZIPs are inefficient for monthly candles
downloaded ZIPs are cached under --cache-dir
--resume is enabled by default and skips locally stored candles
inserts are idempotent through the SQLite unique constraint
```

### Inventory

Show what is stored locally:

```bash
uv run data-fetcher inventory --db-path data/crypto_ohlcv.db
```

Filter inventory:

```bash
uv run data-fetcher inventory \
  --db-path data/crypto_ohlcv.db \
  --exchange binance \
  --symbol BTC/USDT \
  --timeframe 1h
```

### Validate

Check gaps, nulls, duplicates, and invalid price/volume values:

```bash
uv run data-fetcher validate --db-path data/crypto_ohlcv.db
```

Filter validation:

```bash
uv run data-fetcher validate \
  --db-path data/crypto_ohlcv.db \
  --exchange binance \
  --timeframe 1h
```

## SQLite Schema

The canonical table is `price_data`:

```sql
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
```

`price` means close price. Rows are inserted with `INSERT OR IGNORE`, so reruns
are idempotent.

## Backtesting Consumers

`data-fetcher` owns historical download and persistence. Backtesting code can
read the stable price frame contract from the canonical OHLCV table:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path /Users/clas/Documents/trading-repo/data/crypto_ohlcv.db
```

```python
from data_fetcher.storage.sqlite import SQLiteStore

store = SQLiteStore("/Users/clas/Documents/trading-repo/data/crypto_ohlcv.db")
df = store.load_price_frame(
    symbol="BTC/USDT",
    exchange="binance",
    timeframe="1h",
)
```

The returned columns are `milliseconds`, `timestamp`, `symbol`, `price`, and
`volume`, where `price` is the candle close. Reads raise `ValueError` rather
than silently mixing multiple exchanges or timeframes when those filters are
omitted.

For inspection or legacy tooling, export the same contract as CSV:

```bash
uv run data-fetcher export-prices \
  --db-path data/crypto_ohlcv.db \
  --exchange binance \
  --timeframe 1h \
  --symbols BTC/USDT,ETH/USDT \
  --format csv
```

## Python Usage

Fetch directly from a CCXT exchange:

```python
from data_fetcher.providers.crypto import CryptoDataFetcher

fetcher = CryptoDataFetcher(exchange_id="binance")
symbols = fetcher.get_symbols(quote="USDT", active_only=True, spot_only=True)
candles = fetcher.fetch_ohlcv(
    "BTC/USDT",
    timeframe="1h",
    since=0,
    max_requests=1,
)
```

Use the SQLite store:

```python
from data_fetcher.storage.sqlite import SQLiteStore

store = SQLiteStore("data/crypto_ohlcv.db")
inventory = store.get_inventory(exchange="binance", timeframe="1h")
validation = store.validate(exchange="binance", timeframe="1h")
prices = store.load_prices(exchange="binance", timeframe="1h")
```

Legacy imports remain available:

```python
from data_fetcher import CryptoDataFetcher
from data_fetcher.data import AlpacaDataFetcher, PolygonDataFetcher
```

## Legacy CSV Cache

The `csvs/` directory is a legacy cache used by Alpaca and Polygon compatibility
classes. It is not used by the new crypto OHLCV CLI, which writes SQLite
instead.

Older CSV files may have names beginning with `1800-01-01` because previous
Alpaca `earliest` requests used the requested fallback boundary in the filename.
New legacy CSV saves use the actual first and last datetimes returned in the
data.

## Development

Run checks:

```bash
uv run pytest -q
uv run ruff check .
uv run python -m py_compile \
  data_fetcher/cli.py \
  data_fetcher/models.py \
  data_fetcher/providers/crypto.py \
  data_fetcher/storage/sqlite.py \
  data_fetcher/providers/alpaca.py \
  data_fetcher/providers/polygon.py \
  data_fetcher/data.py
```

Normal tests mock exchange calls and do not hit live APIs. The live smoke test
is skipped by default.

## License

MIT. See [pyproject.toml](pyproject.toml) for package metadata.

## Scope

Currently supported:

```text
crypto OHLCV fetching through CCXT
Binance default exchange
Binance public archive ingestion for bulk spot backfills
SQLite persistence
exchanges/symbols/start-dates/fetch/bulk-fetch/inventory/validate CLI
Alpaca and Polygon compatibility classes
```

Not currently included:

```text
parallel fetching
fetch planning reports
start-date reports
Parquet export
full Alpaca/Polygon refactor
live/streaming data
```
