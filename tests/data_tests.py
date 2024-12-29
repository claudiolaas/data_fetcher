import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import pandas as pd
import numpy as np
from data_fetcher.data import (
    BaseDataFetcher,
    CryptoDataFetcher,
    AlpacaDataFetcher,
    PolygonDataFetcher
)

# Fixtures
@pytest.fixture
def mock_df():
    return pd.DataFrame({
        'close': [100, 101, 102],
        'open': [99, 100, 101],
        'high': [101, 102, 103],
        'low': [98, 99, 100],
        'volume': [1000, 1100, 1200]
    })

@pytest.fixture
def crypto_fetcher():
    return CryptoDataFetcher()

@pytest.fixture
def alpaca_fetcher():
    return AlpacaDataFetcher(api_key="test_key", secret_key="test_secret")

@pytest.fixture
def polygon_fetcher():
    return PolygonDataFetcher(api_key="test_key")

# BaseDataFetcher Tests
def test_base_data_fetcher_abstract_methods():
    with pytest.raises(TypeError):
        BaseDataFetcher()

def test_transform_raw_data(mock_df):
    class TestFetcher(BaseDataFetcher):
        def get_data(self): pass
        def get_markets(self): pass
        def get_symbols(self): pass
    
    fetcher = TestFetcher()
    transformed = fetcher.transform_raw_data(mock_df)
    
    assert 'log_return' in transformed.columns
    assert 'asset_return' in transformed.columns
    assert isinstance(transformed['log_return'].iloc[1], float)
    assert isinstance(transformed['asset_return'].iloc[1], float)

def test_save_to_file(tmp_path, mock_df):
    class TestFetcher(BaseDataFetcher):
        def get_data(self): pass
        def get_markets(self): pass
        def get_symbols(self): pass
    
    fetcher = TestFetcher()
    test_file = tmp_path / "test.csv"
    fetcher.save_to_file(mock_df, str(test_file))
    assert test_file.exists()

def test_check_cached_file(tmp_path, mock_df):
    class TestFetcher(BaseDataFetcher):
        def get_data(self): pass
        def get_markets(self): pass
        def get_symbols(self): pass
    
    fetcher = TestFetcher()
    test_file = tmp_path / "test.csv"
    mock_df.to_csv(test_file, index=False)
    
    result = fetcher.check_cached_file(str(test_file))
    assert result is not None
    pd.testing.assert_frame_equal(result, mock_df)

# CryptoDataFetcher Tests
@patch('data_fetcher.data.ccxt.binance')
def test_crypto_fetcher_init(mock_binance):
    fetcher = CryptoDataFetcher()
    mock_binance.assert_called_once()

def test_crypto_get_markets(crypto_fetcher):
    with patch.object(crypto_fetcher.exchange, 'load_markets') as mock_load:
        mock_load.return_value = {'BTC/USDT': {}}
        result = crypto_fetcher.get_markets()
        assert isinstance(result, dict)
        assert 'BTC/USDT' in result

def test_crypto_get_symbols(crypto_fetcher):
    with patch.object(crypto_fetcher.exchange, 'load_markets') as mock_load:
        mock_load.return_value = {'BTC/USDT': {}, 'ETH/USDT': {}}
        result = crypto_fetcher.get_symbols()
        assert isinstance(result, list)
        assert 'BTC/USDT' in result
        assert 'ETH/USDT' in result

def test_crypto_handle_time_boundaries(crypto_fetcher):
    with patch.object(crypto_fetcher, 'get_earliest') as mock_earliest:
        mock_earliest.return_value = 1234567890000
        since, until = crypto_fetcher.handle_time_boundaries(
            start="earliest", 
            end="latest", 
            market="BTC/USDT"
        )
        assert since == 1234567890000
        assert isinstance(until, int)

# AlpacaDataFetcher Tests
def test_alpaca_fetcher_init():
    fetcher = AlpacaDataFetcher(api_key="test_key", secret_key="test_secret")
    assert fetcher.api_key == "test_key"
    assert fetcher.secret_key == "test_secret"

def test_alpaca_get_symbols(alpaca_fetcher):
    with patch.object(alpaca_fetcher.alpaca_rest, 'list_assets') as mock_list:
        mock_list.return_value = [Mock(symbol='AAPL'), Mock(symbol='MSFT')]
        result = alpaca_fetcher.get_symbols()
        assert isinstance(result, list)
        assert 'AAPL' in result
        assert 'MSFT' in result

def test_alpaca_handle_time_boundaries(alpaca_fetcher):
    since, until = alpaca_fetcher.handle_time_boundaries(
        start="earliest",
        end="latest"
    )
    assert isinstance(since, datetime)
    assert isinstance(until, datetime)

# PolygonDataFetcher Tests
def test_polygon_fetcher_init():
    fetcher = PolygonDataFetcher(api_key="test_key")
    assert fetcher.api_key == "test_key"

@patch('data_fetcher.data.requests.get')
def test_polygon_get_data(mock_get, polygon_fetcher):
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
    
    df = polygon_fetcher.get_data(
        ticker='AAPL',
        start_date='2023-01-01',
        end_date='2023-01-02'
    )
    
    assert isinstance(df, pd.DataFrame)
    assert 'close' in df.columns
    assert 'open' in df.columns
