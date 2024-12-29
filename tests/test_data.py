import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from data_fetcher.data import (
    BaseDataFetcher,
    CryptoDataFetcher,
    AlpacaDataFetcher,
    PolygonDataFetcher
)

# Fixtures
@pytest.fixture
def crypto_fetcher():
    return CryptoDataFetcher()

@pytest.fixture
def alpaca_fetcher():
    return AlpacaDataFetcher(api_key="test_key", secret_key="test_secret")

@pytest.fixture
def polygon_fetcher():
    return PolygonDataFetcher(api_key="test_key")

# BaseDataFetcher tests
def test_base_data_fetcher_abstract_methods():
    with pytest.raises(TypeError):
        BaseDataFetcher()

# CryptoDataFetcher tests
def test_crypto_fetcher_init(crypto_fetcher):
    assert crypto_fetcher.exchange is not None

def test_crypto_fetcher_get_markets(crypto_fetcher):
    with patch.object(crypto_fetcher.exchange, 'load_markets') as mock_load:
        mock_load.return_value = {'BTC/USDT': {}}
        markets = crypto_fetcher.get_markets()
        assert isinstance(markets, dict)
        assert 'BTC/USDT' in markets

def test_crypto_fetcher_get_symbols(crypto_fetcher):
    with patch.object(crypto_fetcher.exchange, 'load_markets') as mock_load:
        mock_load.return_value = {'BTC/USDT': {}, 'ETH/USDT': {}}
        symbols = crypto_fetcher.get_symbols()
        assert isinstance(symbols, list)
        assert 'BTC/USDT' in symbols
        assert 'ETH/USDT' in symbols

# AlpacaDataFetcher tests
def test_alpaca_fetcher_init(alpaca_fetcher):
    assert alpaca_fetcher.api_key == "test_key"
    assert alpaca_fetcher.secret_key == "test_secret"

def test_alpaca_fetcher_get_symbols(alpaca_fetcher):
    with patch.object(alpaca_fetcher.alpaca_rest, 'list_assets') as mock_list:
        mock_list.return_value = [Mock(symbol='AAPL'), Mock(symbol='GOOG')]
        symbols = alpaca_fetcher.get_symbols()
        assert isinstance(symbols, list)
        assert 'AAPL' in symbols
        assert 'GOOG' in symbols

# PolygonDataFetcher tests
def test_polygon_fetcher_init(polygon_fetcher):
    assert polygon_fetcher.api_key == "test_key"

def test_polygon_fetcher_get_data(polygon_fetcher):
    with patch('requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = {
            'status': 'OK',
            'results': [{
                't': 1672531200000,
                'c': 100,
                'o': 99,
                'h': 101,
                'l': 98,
                'v': 1000
            }]
        }
        mock_get.return_value = mock_response
        
        df = polygon_fetcher.get_data(ticker='AAPL')
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert 'close' in df.columns
