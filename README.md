# data-fetcher

Reusable market-data fetching utilities for research projects.

The current production path is a crypto OHLCV command-line tool built on
`ccxt`. It discovers symbols, fetches historical candles, stores them in a
resumable SQLite database, and validates local data quality. Binance is the
default exchange, but any CCXT exchange id can be selected.

Alpaca and Polygon compatibility classes are still available, but their deeper
refactor is intentionally deferred.

## Installation

From a local checkout:

```bash
git clone https://github.com/claudiolaas/data_fetcher.git
cd data_fetcher
uv venv
uv pip install -e ".[crypto,dev]"
```

For optional provider dependencies:

```bash
uv pip install -e ".[alpaca,polygon]"
```

Useful extras:

```text
crypto   ccxt-based crypto fetching
alpaca   Alpaca compatibility provider
polygon  Polygon compatibility provider
all      all provider dependencies
dev      pytest, ruff, and local development tools
```

## CLI

The package exposes one command:

```bash
uv run data-fetcher --help
```

Available Phase 1 commands:

```text
exchanges
symbols
fetch
inventory
validate
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
```

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

## Scope

Phase 1 includes:

```text
crypto OHLCV fetching through CCXT
Binance default exchange
SQLite persistence
symbols/fetch/inventory/validate CLI
Alpaca and Polygon compatibility classes
```

Deferred:

```text
parallel fetching
fetch planning reports
start-date reports
Parquet export
full Alpaca/Polygon refactor
live/streaming data
```
