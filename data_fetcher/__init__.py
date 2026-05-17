# Compatibility layer: keep old imports working
try:
    from .data import (  # noqa: F401
        BaseDataFetcher,
        CryptoDataFetcher,
        AlpacaDataFetcher,
        PolygonDataFetcher,
    )
except ImportError:
    # Optional dependencies (alpaca, requests) may not be installed
    pass

__all__ = [
    "BaseDataFetcher",
    "CryptoDataFetcher",
    "AlpacaDataFetcher",
    "PolygonDataFetcher",
]
