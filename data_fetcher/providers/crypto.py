"""Crypto OHLCV data provider using CCXT."""

import ccxt
import logging
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

DEFAULT_EXCHANGE = "binance"


def create_exchange(exchange_id: str = DEFAULT_EXCHANGE) -> ccxt.Exchange:
    exchange_class = getattr(ccxt, exchange_id, None)
    if exchange_class is None:
        raise ValueError(f"Unknown CCXT exchange: {exchange_id}")
    exchange = exchange_class()
    exchange.load_markets()
    return exchange


class CryptoDataFetcher:
    """Crypto data fetcher wrapping a CCXT exchange for OHLCV retrieval."""

    def __init__(self, exchange_id: str = DEFAULT_EXCHANGE):
        self.exchange_id = exchange_id
        self.exchange = create_exchange(exchange_id)
        self._markets: Dict = self.exchange.markets

    def get_markets(self) -> Dict:
        return self._markets

    def get_ticker(self) -> List[str]:
        return list(self._markets.keys())

    def get_symbols(
        self,
        quote: Optional[str] = None,
        active_only: bool = True,
        spot_only: bool = True,
    ) -> List[Dict]:
        results = []
        for sym, info in self._markets.items():
            if active_only and not info.get("active", False):
                continue
            if spot_only and info.get("type") != "spot":
                continue
            if quote and info.get("quote") != quote:
                continue
            limits = info.get("limits", {})
            amount_limit = limits.get("amount", {})
            cost_limit = limits.get("cost", {})
            results.append({
                "symbol": sym,
                "base": info.get("base"),
                "quote": info.get("quote"),
                "type": info.get("type", "spot"),
                "active": info.get("active", True),
                "precision": info.get("precision", {}),
                "min_amount": amount_limit.get("min"),
                "min_cost": cost_limit.get("min"),
            })
        return results

    def fetch_earliest_timestamp(
        self, symbol: str, timeframe: str = "1h", max_probes: int = 20
    ) -> Tuple[Optional[int], str]:
        # Strategy 1: try since=0 with limit=1
        try:
            candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=0, limit=1)
            if candles and len(candles) > 0:
                ts = int(candles[0][0])
                now_ms = int(self.exchange.milliseconds())
                if ts < now_ms - 86400_000 * 90:
                    return ts, "since_zero"
        except Exception:
            pass

        # Strategy 2: bounded search
        current_raw = self.exchange.parse8601("2013-01-01T00:00:00Z")
        if current_raw is None:
            return None, "failed"
        current: int = int(current_raw)
        now_ms = int(self.exchange.milliseconds())
        probes = 0

        while current < now_ms and probes < max_probes:
            try:
                candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=current, limit=1)
                probes += 1
                if candles and len(candles) > 0:
                    return int(candles[0][0]), "probe_search"
                current += 86400_000 * 30
            except Exception:
                current += 86400_000 * 30
                probes += 1

        return None, "failed"

    def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1h",
        since: Optional[int] = None,
        until: Optional[int] = None,
        limit: int = 1000,
        max_requests: Optional[int] = None,
        sleep_seconds: float = 0.0,
    ) -> List[List[Union[int, float]]]:
        """Fetch OHLCV candles for a symbol.

        Parameters
        ----------
        symbol : str
            Trading pair symbol (e.g. "BTC/USDT").
        timeframe : str, default "1h"
            Candle timeframe.
        since : int, optional
            Start timestamp in milliseconds. If None, fetches from the
            earliest available data.
        until : int, optional
            Upper bound timestamp in milliseconds. Fetches stop once all
            returned candles are >= this value.
        limit : int, default 1000
            Number of candles per API request (max depends on exchange).
        max_requests : int, optional
            Maximum number of API requests per symbol. If None, fetches
            all available.
        sleep_seconds : float, default 0.0
            Seconds to sleep between pagination requests.

        Returns
        -------
        list of list
            OHLCV rows as [[ms, open, high, low, close, volume], ...].
        """
        all_candles: List[List[Union[int, float]]] = []
        request_count = 0

        while True:
            if max_requests is not None and request_count >= max_requests:
                break

            try:
                candles = self.exchange.fetch_ohlcv(
                    symbol, timeframe=timeframe, since=since, limit=limit
                )
                request_count += 1
            except Exception as e:
                logger.warning("Error fetching %s at %s: %s", symbol, since, e)
                break

            if not candles or len(candles) == 0:
                break

            # Filter out candles beyond until before extending
            if until is not None:
                candles = [c for c in candles if int(c[0]) <= until]

            if not candles:
                break

            all_candles.extend(candles)

            if not candles:
                break

            last_ts = int(candles[-1][0])

            # Stop if since didn't advance
            if since is not None and last_ts <= since:
                break

            # Stop if we've reached the until bound
            if until is not None and last_ts >= until:
                break

            since = last_ts + 1

            # If fewer than limit returned, this is the last page
            if len(candles) < limit:
                break

            # Per-request pacing to respect exchange rate limits
            if sleep_seconds > 0:
                import time
                time.sleep(sleep_seconds)

        return all_candles

    def get_earliest(self, market: str) -> int:
        ts, _ = self.fetch_earliest_timestamp(market)
        if ts is None:
            return int(self.exchange.milliseconds())
        return ts
