"""Data models for OHLCV data."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class OHLCV:
    """A single OHLCV candle."""
    milliseconds: int
    timestamp: str
    exchange: str
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    price: float  # close price
    volume: float


@dataclass
class InventoryRow:
    """Summary of stored data for a exchange/symbol/timeframe combination."""
    exchange: str
    symbol: str
    timeframe: str
    rows: int
    first_datetime_utc: Optional[str] = None
    last_datetime_utc: Optional[str] = None


@dataclass
class ValidationResult:
    """Validation results for a exchange/symbol/timeframe combination."""
    exchange: str
    symbol: str
    timeframe: str
    rows: int
    duplicate_count: int = 0
    null_count: int = 0
    non_positive_price_count: int = 0
    non_positive_volume_count: int = 0
    expected_interval_ms: int = 3600000
    gap_count: int = 0
    largest_gap_bars: int = 0
    first_datetime_utc: Optional[str] = None
    last_datetime_utc: Optional[str] = None
    status: str = "PASS"
