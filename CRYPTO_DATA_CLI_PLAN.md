# Crypto Data CLI Plan

This plan defines a central historical crypto data utility for the repository.
The immediate goal is to fetch more symbols and more history for V8/V7 style
research without maintaining one-off scripts.

## External Package Candidate

There is an older side project:

```text
https://github.com/claudiolaas/data_fetcher.git
```

Local review summary:

```text
package name: data_fetcher
version: 0.1.9
main module: data_fetcher/data.py
providers:
  CryptoDataFetcher
  AlpacaDataFetcher
  PolygonDataFetcher
storage:
  CSV cache under csvs/
packaging:
  setup.py
  console script points to data_fetcher.cli:main, but cli.py is not present
```

Useful ideas already present:

```text
BaseDataFetcher abstraction
provider-specific fetchers
ccxt-based crypto fetching
market/ticker discovery
start/end boundary handling
basic cache layer
Alpaca and Polygon support for future non-crypto use
```

Important gaps for this repo's current needs:

```text
no resumable SQLite store
no OHLCV database schema
no central working CLI
no exchange selection for crypto; Binance is hard-coded
no multi-symbol fetch orchestration
no inventory / validation / fetch planning
no robust data reports
tests reference get_symbols(), but implementation exposes get_ticker()
CSV cache path is fixed to csvs/
dependencies pull in Alpaca packages even if only crypto is needed
```

Conclusion:

```text
Do not duplicate this inside trading_repo.
Use the side project as the installable package home, but evolve it
substantially.
```

The best architecture is:

```text
data_fetcher package:
  owns provider clients, fetching, persistence, validation, CLI

trading_repo:
  depends on data_fetcher only for data acquisition
  keeps DataLoader / PriceRepository for backtesting reads
  optionally adds import helpers for data_fetcher SQLite outputs
```

This makes the data utility reusable across repositories while keeping
`trading_repo` focused on modeling and backtesting.

## Current State

The repo currently has three data-related paths:

```text
DataLoader / PriceRepository
  read-side path used by backtests and experiments

trading_repo.data.data_fetcher
  older async fetcher with YAML config and Redis control messages

scripts/fetch_hourly_volume.py
  newer one-off Binance OHLCV backfill script
```

The most useful fetch implementation today is `scripts/fetch_hourly_volume.py`:

```text
strengths:
  fetches OHLCV, not only close
  stores volume
  resumable by symbol
  uses INSERT OR IGNORE
  fetches earliest available history
  has simple progress logging

limitations:
  hard-coded Binance
  hard-coded 26 symbols
  hard-coded 1h timeframe
  hard-coded database path
  no symbol discovery
  no start-date inspection
  no dry-run / planning mode
  no central CLI entrypoint
```

The older `trading_repo.data.data_fetcher` should not be the base for the new
historical utility:

```text
reasons:
  tied to Redis
  mixes historical backfill and continuous live fetching
  stores only close price through DatabaseManager
  comments say 1-minute base data but implementation fetches 1h
  default path handling is stale
```

Keep it as a separate live/legacy path for now. The new module should focus on
historical OHLCV research data.

## Target Shape

Preferred target:

```text
data_fetcher/
  pyproject.toml
  data_fetcher/
    __init__.py
    cli.py
    models.py
    providers/
      __init__.py
      crypto.py
      alpaca.py
      polygon.py
    storage/
      __init__.py
      sqlite.py
      csv.py
    validation.py
    planning.py
    symbols.py
```

Expose package CLI:

```toml
[project.scripts]
data-fetcher = "data_fetcher.cli:app"
```

Then `trading_repo` can add it as a dependency later:

```toml
dependencies = [
  "data-fetcher @ git+https://github.com/claudiolaas/data_fetcher.git",
]
```

or as an editable local dependency while developing both repos:

```bash
uv pip install -e ../data_fetcher
```

Fallback target if we decide not to split yet:

```text
trading_repo/data/crypto_data/
  __init__.py
  cli.py
  exchange.py
  store.py
  planning.py
  models.py
```

Expose fallback through `pyproject.toml`:

```toml
[project.scripts]
trading-repo = "trading_repo.backtesting.runner:app"
trading-data = "trading_repo.data.crypto_data.cli:app"
```

Recommendation:

```text
Build in data_fetcher, not trading_repo.
```

The utility should use `typer`, matching the main backtesting CLI style. The
package CLI should be named `data-fetcher`; if `trading_repo` needs a local
alias later, it can expose `trading-data` as a thin wrapper.

## Database Contract

Use one canonical SQLite table:

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

Indexes:

```sql
CREATE INDEX IF NOT EXISTS idx_price_data_symbol
ON price_data(exchange, symbol, timeframe);

CREATE INDEX IF NOT EXISTS idx_price_data_time
ON price_data(milliseconds);
```

Compatibility concern:

Current backtests read legacy databases where `price_data` may only contain:

```text
milliseconds, timestamp, symbol, price, optional volume
```

So the first implementation should either:

```text
Option A:
  make PriceRepository handle both schemas

Option B:
  write new expanded databases but provide a compatibility view:
    SELECT milliseconds, timestamp, symbol, price, volume FROM price_data
```

Recommendation:

```text
Start with Option A.
```

`PriceRepository` can detect whether `exchange` and `timeframe` columns exist.
When absent, it behaves exactly as it does today. When present, it accepts
optional exchange/timeframe filters.

## CLI Commands

### 1. List Exchanges

Purpose:

```text
Show supported ccxt exchange IDs and highlight the default.
```

Command:

```bash
uv run data-fetcher exchanges
```

Output fields:

```text
exchange
has_fetch_ohlcv
rate_limit_ms
default
```

Initial support:

```text
binance
coinbase
kraken
okx
bybit
```

Binance remains the default.

### 2. List Available Symbols

Purpose:

```text
Discover exchange symbols before deciding what to fetch.
```

Command:

```bash
uv run data-fetcher symbols \
  --exchange binance \
  --quote USDT \
  --spot-only \
  --active-only
```

Useful options:

```text
--exchange binance
--quote USDT
--base BTC
--contains BTC
--spot-only / --swap-only
--active-only / --include-inactive
--limit 200
--output table|csv|json
```

Output fields:

```text
symbol
base
quote
type
active
precision
min_amount
min_cost
```

### 3. Inspect Exchange Start Dates

Purpose:

```text
Estimate earliest available OHLCV timestamp for one or many symbols.
```

Command:

```bash
uv run data-fetcher start-dates \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT,FET/USDT \
  --timeframe 1h
```

Useful options:

```text
--symbols BTC/USDT,ETH/USDT
--symbols-file symbols.txt
--quote USDT
--timeframe 1h
--max-probes 20
--output table|csv|json
```

Implementation note:

CCXT does not expose a reliable universal listing/start date. For OHLCV, the
practical method is probing `fetch_ohlcv(symbol, timeframe, since=0, limit=1)`
and falling back to bounded search if an exchange ignores `since=0`.

Output fields:

```text
exchange
symbol
timeframe
first_timestamp
first_datetime_utc
probe_method
status
error
```

### 4. Database Inventory

Purpose:

```text
Show what is already stored locally.
```

Command:

```bash
uv run data-fetcher inventory \
  --db-path data/crypto_ohlcv.db
```

Useful options:

```text
--exchange binance
--symbol BTC/USDT
--timeframe 1h
--output table|csv|json
```

Output fields:

```text
exchange
symbol
timeframe
rows
first_timestamp
first_datetime_utc
last_timestamp
last_datetime_utc
missing_bar_count
coverage_pct
```

This replaces ad hoc DB checks and tells us whether a rerun will resume or start
fresh.

### 5. Plan Fetch

Purpose:

```text
Dry-run a fetch request before spending hours downloading data.
```

Command:

```bash
uv run data-fetcher plan-fetch \
  --exchange binance \
  --symbols-file symbols.txt \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Output fields:

```text
symbol
local_start
local_end
exchange_start
planned_since
planned_until
estimated_new_bars
estimated_requests
status
```

### 6. Fetch OHLCV

Purpose:

```text
Fetch historical candles for one or many symbols into SQLite.
```

Command:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Useful options:

```text
--symbols BTC/USDT,ETH/USDT
--symbols-file symbols.txt
--quote USDT
--limit-symbols 100
--timeframe 1h
--since earliest|YYYY-MM-DD|milliseconds
--until now|YYYY-MM-DD|milliseconds
--resume
--overwrite
--max-requests-per-symbol
--sleep-seconds 0.12
--workers 1
--fail-fast
--output-dir data/fetch_reports/YYYYMMDD_HHMMSS
```

Recommendation:

```text
Start with --workers 1.
```

Most CCXT exchanges have strict rate limits. Parallelism can be added later via
a central per-exchange rate limiter, but serial fetching is simpler and safer
for large backfills.

Fetch behavior:

```text
if --resume:
  start from max stored timestamp for exchange/symbol/timeframe

if no local data and --since earliest:
  request earliest available OHLCV

if --overwrite:
  delete existing rows for exchange/symbol/timeframe before fetching

always:
  insert with UNIQUE protection
  write progress
  write a fetch report
```

### 7. Validate Data

Purpose:

```text
Check row spacing, duplicates, nulls, and obvious gaps.
```

Command:

```bash
uv run data-fetcher validate \
  --db-path data/crypto_ohlcv.db \
  --exchange binance \
  --timeframe 1h
```

Output fields:

```text
symbol
rows
duplicate_count
null_count
non_positive_price_count
non_positive_volume_count
expected_interval_ms
gap_count
largest_gap_bars
first_datetime_utc
last_datetime_utc
status
```

### 8. Export Symbols

Purpose:

```text
Create symbol files for large experiments.
```

Command:

```bash
uv run data-fetcher export-symbols \
  --db-path data/crypto_ohlcv.db \
  --exchange binance \
  --quote USDT \
  --min-rows 50000 \
  --min-start 2020-01-01 \
  --output data/symbol_sets/binance_usdt_hourly_liquid.txt
```

Useful filters:

```text
--min-rows
--min-start
--max-start
--min-end
--quote USDT
--exclude-stablecoins
--top-by-volume 200
```

This is the bridge from data collection to V8/V9/V10 experiments.

## Implementation Phases

### Phase 1: Historical Backfill Core

Deliver:

```text
data_fetcher/cli.py
data_fetcher/providers/crypto.py
data_fetcher/storage/sqlite.py
data_fetcher/models.py
```

Commands:

```text
symbols
inventory
fetch
validate
```

Port logic from `scripts/fetch_hourly_volume.py` into reusable classes inside
the external `data_fetcher` package. Keep the existing `CryptoDataFetcher`
interface as a compatibility layer if possible, but do not preserve the fixed
CSV-only behavior as the primary path.

Tests:

```text
store schema creation
insert idempotency
inventory summary
validation detects gaps
symbol parsing from CLI
```

### Phase 2: Planning And Discovery

Deliver:

```text
start-dates
plan-fetch
export-symbols
```

Add fetch reports:

```text
fetch_config.json
fetch_summary.csv
fetch_errors.csv
```

### Phase 3: Reader Integration

Update:

```text
trading_repo pyproject dependency
PriceRepository
DataLoader
SymbolResolver
```

Goals:

```text
read expanded OHLCV schema
preserve legacy DB compatibility
allow optional exchange/timeframe filtering
support symbol specs from exported symbol files
```

### Phase 4: Scale And Operations

Only after the serial utility is reliable:

```text
rate-limited concurrent fetching
retry policy with exponential backoff
resume manifests
exchange-specific quirks
delisting/inactive symbol handling
optional Parquet export
```

## Recommended Defaults

```text
exchange: binance
market: spot
quote: USDT
timeframe: 1h
db_path: data/crypto_ohlcv.db
since: earliest
resume: true
sleep_seconds: exchange.rateLimit / 1000, minimum 0.12 for Binance
workers: 1
```

## Open Questions

1. Should the canonical research database include all exchanges in one DB, or
   one DB per exchange?

   Recommendation: one DB with an `exchange` column. This makes cross-exchange
   comparison possible and avoids path proliferation.

2. Should backtests read multi-exchange symbols as `BINANCE:BTC/USDT`?

   Recommendation: keep plain symbols for single-exchange runs and add explicit
   exchange filtering in `DataLoader` later. Avoid changing strategy-facing
   symbol names until needed.

3. Should we fetch stablecoin/stablecoin and leveraged tokens?

   Recommendation: default them out for experiments. Keep CLI options to include
   them.

4. Should volume become part of core strategy data?

   Recommendation: no immediate change. Store OHLCV, but keep `RunData` close-
   price-first. Volume can enter through `RunData.other` or future feature
   engines when explicitly used.

5. Should the old Redis data fetcher be removed?

   Recommendation: not in this phase. Mark it legacy after the new CLI exists
   and remove only when no workflow depends on it.

## First Implementation Step

Implement Phase 1 in `https://github.com/claudiolaas/data_fetcher.git` with
Binance support and these commands:

```bash
uv run data-fetcher symbols --exchange binance --quote USDT --active-only
uv run data-fetcher fetch --exchange binance --symbols BTC/USDT --timeframe 1h --since earliest
uv run data-fetcher inventory --db-path data/crypto_ohlcv.db
uv run data-fetcher validate --db-path data/crypto_ohlcv.db
```

Once that works for one symbol, test a small basket:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT,FET/USDT,CELR/USDT,THETA/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

## Handoff Acceptance Criteria

This section is intended for another coding agent or a future implementation
session. It removes ambiguity from Phase 1.

### Naming

Use these names exactly:

```text
Python import package: data_fetcher
Python distribution:   data-fetcher
CLI executable:        data-fetcher
Git branch:            crypto-ohlcv-cli
```

Do not introduce a second command name such as `trading-data` in Phase 1.
`trading_repo` may add a wrapper later if needed.

### Phase 1 Scope

Implement only:

```text
crypto OHLCV fetching through ccxt
Binance as default exchange
exchange selection by ccxt exchange id
SQLite persistence
symbols command
fetch command
inventory command
validate command
tests for the above
```

Do not implement in Phase 1:

```text
Alpaca refactor
Polygon refactor
live/streaming fetch
Redis integration
parallel fetching
Parquet export
trading_repo integration
target/model/backtest changes
```

### Alpaca And Polygon Compatibility

Keep Alpaca and Polygon in the package, but do not make them blockers for the
crypto implementation.

Required behavior:

```text
existing imports should continue to work where practical:
  from data_fetcher.data import CryptoDataFetcher
  from data_fetcher.data import AlpacaDataFetcher
  from data_fetcher.data import PolygonDataFetcher
```

Recommended compatibility shim:

```python
# data_fetcher/data.py
from data_fetcher.providers.crypto import CryptoDataFetcher
from data_fetcher.providers.alpaca import AlpacaDataFetcher
from data_fetcher.providers.polygon import PolygonDataFetcher
```

Dependency split:

```toml
[project.optional-dependencies]
crypto = ["ccxt"]
alpaca = ["alpaca-py", "alpaca-trade-api"]
polygon = ["requests"]
all = ["ccxt", "alpaca-py", "alpaca-trade-api", "requests"]
dev = ["pytest", "typer", "ruff"]
```

Crypto-only development should work with:

```bash
uv pip install -e ".[crypto,dev]"
```

### Storage Contract

Use SQLite table `price_data`.

Column meaning:

```text
price = close
```

Schema:

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

Insert behavior:

```text
idempotent inserts
safe reruns
resume from MAX(milliseconds) for exchange/symbol/timeframe
store timestamps as UTC text
```

### CLI Minimum Behavior

`symbols`:

```bash
uv run data-fetcher symbols --exchange binance --quote USDT --active-only
```

Must:

```text
load ccxt markets
filter by quote
filter inactive markets by default when --active-only is passed
print symbol/base/quote/type/active
```

`fetch`:

```bash
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Must:

```text
create database if missing
fetch OHLCV batches with limit 1000 by default
resume from local max timestamp unless --overwrite is passed
insert open/high/low/close-as-price/volume
print progress per symbol
continue safely on rerun without duplicate rows
```

`inventory`:

```bash
uv run data-fetcher inventory --db-path data/crypto_ohlcv.db
```

Must report:

```text
exchange
symbol
timeframe
rows
first_datetime_utc
last_datetime_utc
```

`validate`:

```bash
uv run data-fetcher validate --db-path data/crypto_ohlcv.db
```

Must report:

```text
duplicate_count
null_count
non_positive_price_count
non_positive_volume_count
gap_count
largest_gap_bars
status
```

### CLI Output

Do not overbuild output formatting in Phase 1.

Required:

```text
human-readable table/text output
```

Nice but optional:

```text
--output csv
--output json
```

### Minimum Tests

Implement tests with mocks and temporary SQLite files. Do not hit the live
exchange in normal tests.

Required tests:

```text
SQLite schema creation
idempotent OHLCV insert
resume timestamp lookup
inventory from temp DB
validate detects missing hourly bar
validate detects non-positive price or volume
symbol filtering from mocked ccxt markets
fetch loop from mocked OHLCV batches
Typer CLI smoke for symbols/inventory/validate
```

Optional live smoke test:

```text
marked or skipped by default
fetch BTC/USDT 1h with max one request
```

### Definition Of Done

Phase 1 is done when all of this works:

```bash
cd /Users/clas/Documents/data_fetcher
uv pip install -e ".[crypto,dev]"
uv run pytest
uv run data-fetcher --help
uv run data-fetcher symbols --exchange binance --quote USDT --active-only
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path /tmp/crypto_ohlcv_smoke.db \
  --max-requests-per-symbol 1
uv run data-fetcher inventory --db-path /tmp/crypto_ohlcv_smoke.db
uv run data-fetcher validate --db-path /tmp/crypto_ohlcv_smoke.db
```

And from `trading_repo`:

```bash
cd /Users/clas/Documents/trading-repo
uv pip install -e ../data_fetcher
uv run data-fetcher inventory --db-path /tmp/crypto_ohlcv_smoke.db
```

No `trading_repo` code changes are required for Phase 1 beyond this planning
document.

## Development Workflow For `data_fetcher`

Work on the external package as a sibling repository, not inside
`trading_repo`.

### 1. Clone The Package

```bash
cd /Users/clas/Documents
git clone https://github.com/claudiolaas/data_fetcher.git
cd data_fetcher
```

Create a focused branch:

```bash
git checkout -b crypto-ohlcv-cli
```

### 2. Modernize Package Structure

Initial package target:

```text
data_fetcher/
  pyproject.toml
  data_fetcher/
    __init__.py
    cli.py
    models.py
    providers/
      __init__.py
      crypto.py
      alpaca.py
      polygon.py
    storage/
      __init__.py
      sqlite.py
      csv.py
    validation.py
    planning.py
    symbols.py
  tests/
```

Keep `data_fetcher/data.py` temporarily as a compatibility layer so old imports
do not break immediately:

```python
from data_fetcher.providers.crypto import CryptoDataFetcher
from data_fetcher.providers.alpaca import AlpacaDataFetcher
from data_fetcher.providers.polygon import PolygonDataFetcher
```

### 3. Develop Locally

Use an editable install while developing:

```bash
cd /Users/clas/Documents/data_fetcher
uv venv
uv pip install -e ".[dev]"
uv run pytest
uv run data-fetcher --help
```

First smoke commands:

```bash
uv run data-fetcher symbols --exchange binance --quote USDT --active-only
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path /Users/clas/Documents/trading-repo/data/crypto_ohlcv.db
uv run data-fetcher inventory \
  --db-path /Users/clas/Documents/trading-repo/data/crypto_ohlcv.db
uv run data-fetcher validate \
  --db-path /Users/clas/Documents/trading-repo/data/crypto_ohlcv.db
```

### 4. Test From `trading_repo`

Install the sibling package into the `trading_repo` environment:

```bash
cd /Users/clas/Documents/trading-repo
uv pip install -e ../data_fetcher
```

Then run the package CLI from the `trading_repo` context:

```bash
uv run data-fetcher symbols --exchange binance --quote USDT --active-only
uv run data-fetcher fetch \
  --exchange binance \
  --symbols BTC/USDT,ETH/USDT,FET/USDT,CELR/USDT,THETA/USDT \
  --timeframe 1h \
  --since earliest \
  --db-path data/crypto_ohlcv.db
```

Do not add `data_fetcher` as a hard dependency in `trading_repo` until the new
CLI is stable. During development, editable install is enough.

### 5. Push Back To GitHub

When the package works locally:

```bash
cd /Users/clas/Documents/data_fetcher
git status
git add .
git commit -m "Add crypto OHLCV SQLite CLI"
git push -u origin crypto-ohlcv-cli
```

After review, either:

```text
merge the branch into main
```

or temporarily consume the branch from `trading_repo`:

```toml
"data-fetcher @ git+https://github.com/claudiolaas/data_fetcher.git@crypto-ohlcv-cli"
```

Longer term, publish a proper version once the CLI and storage contract settle.
