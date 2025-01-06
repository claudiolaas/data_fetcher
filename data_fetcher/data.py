import time
import requests
import ccxt
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from alpaca.data.historical import StockHistoricalDataClient
from datetime import datetime, timedelta
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca_trade_api.rest import REST
import os

from abc import ABC, abstractmethod

from typing import Dict, List, Optional, Union
import pandas as pd

class BaseDataFetcher(ABC):

    @abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_markets(self) -> Dict:
        pass

    @abstractmethod
    def get_ticker(self) -> List[str]:
        pass

    @abstractmethod
    def transform_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def get_earliest(self, market: str) -> int:
        """Get the earliest available timestamp for a given market"""
        since = self.exchange.parse8601('2010-01-01' + "T00:00:00Z")
        until = self.exchange.parse8601('2050-01-01' + "T00:00:00Z")
        while since < until:
            orders = self.exchange.fetchOHLCV(market, timeframe='1M', since=since)
            if len(orders) > 0:
                return orders[0][0]
            since += (1000 * 60 * 60 * 24 * 30)  # shift forward 30 days
        return until

    def save_to_file(self, df: pd.DataFrame, filename: str) -> None:
        my_file = Path("csvs/") / filename
        if not Path("csvs/").is_dir():
            Path("csvs/").mkdir()
        df.to_csv(my_file, index=False)

    def check_cached_file(self, filename: str) -> Optional[pd.DataFrame]:
        my_file = Path("csvs/") / filename
        if my_file.is_file():
            return pd.read_csv(my_file)
        return None
    
    def transform_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()  # Avoid chained assignment warning
        df["log_return"] = np.log(df["close"]).diff().fillna(0)
        df["asset_return"] = df["close"] / df["close"].shift()
        df["asset_return"] = df["asset_return"].fillna(1)
        return df


class CryptoDataFetcher(BaseDataFetcher):
    def __init__(self):
        self.exchange = ccxt.binance()

    def get_markets(self) -> Dict:
        markets = self.exchange.load_markets()
        return markets  
    
    def get_ticker(self) -> List[str]:
        markets = self.exchange.load_markets()
        return list(markets.keys())
    

    def handle_time_boundaries(self, start: str, end: str, market: str) -> tuple[int, int]:
        earliest = self.get_earliest(market)
        today = datetime.today().strftime("%Y-%m-%d")
        latest = self.exchange.parse8601(today + "T00:00:00Z")

        since, until = None, None
        if start == "earliest":
            since = earliest
        else:
            since = max(self.exchange.parse8601(start + "T00:00:00Z"), earliest)

        if end == "latest":
            until = latest
        else:
            until = min(self.exchange.parse8601(end + "T00:00:00Z"), latest)

        return since, until
    
    def fetch_exchange_data(self, market: str, timeframe: str, since: int, until: int) -> List[List[Union[int, float]]]:
        all_orders = []
        while since < until:
            orders = self.exchange.fetchOHLCV(market, timeframe=timeframe, since=since)
            if len(orders) > 0:
                latest_fetched = orders[-1][0]
                if since == latest_fetched:
                    break
                else:
                    since = latest_fetched
                all_orders += orders
            else:
                since += (1000 * 60 * 60 * 24)
        return all_orders
    
    def get_data(self,ticker: str = "BTC/USDT", start_date: str = "earliest", end_date: str = "latest",  step: str = "1h") -> pd.DataFrame:
        since, until = self.handle_time_boundaries(start_date, end_date, ticker)
        since_str = self.exchange.iso8601(since)[:10]
        until_str = self.exchange.iso8601(until)[:10]
        
        filename = f'{ticker.replace("/","-")}_{step}_{since_str}_{until_str}.csv'
        cached_df = self.check_cached_file(filename)
        if cached_df is not None:
            print(f'cached {filename}')
            return cached_df
        else:
            df_list = self.fetch_exchange_data(ticker, step, since, until)
            df = pd.DataFrame(df_list).reset_index()
            df.rename(columns={0: "milliseconds", 1: "open", 2: "high", 3: "low", 4: "close", 5: "volume"}, inplace=True)
            df["dt"] = df["milliseconds"].apply(lambda x: self.exchange.iso8601(x))
            df = self.transform_raw_data(df)
            self.save_to_file(df, filename)
            return df
        
class AlpacaDataFetcher(BaseDataFetcher):
    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.getenv('ALPACA_API_KEY')
        self.secret_key = secret_key or os.getenv('ALPACA_SECRET_KEY')

        if not self.api_key or not self.secret_key:
            raise ValueError('API key and secret key must be provided either as arguments or as environment variables.')

        self.alpaca_client = StockHistoricalDataClient(api_key=self.api_key, secret_key=self.secret_key)
        self.alpaca_rest = REST(self.api_key, self.secret_key) 
        
    def get_ticker(self) -> List[str]:
        assets =  self.alpaca_rest.list_assets()
        return [asset.symbol for asset in assets]
    
    def get_markets(self) -> List:
        return self.alpaca_rest.list_assets()
    

    def handle_time_boundaries(self, start: str, end: str) -> tuple[datetime, datetime]:
        earliest = datetime.strptime("1800-01-01", '%Y-%m-%d')
        latest = datetime.today() - timedelta(days=1)
        since = earliest if start == "earliest" else datetime.strptime(start, '%Y-%m-%d')
        until = latest if end == "latest" else datetime.strptime(end, '%Y-%m-%d')
        return since, until
    
    def fetch_alpaca_data(self, symbol: str, since: datetime, until: datetime, step: str) -> pd.DataFrame:
        step_dict = {
            '1m': TimeFrame.Minute,
            '1h': TimeFrame.Hour,
            '1d': TimeFrame.Day,
            '1w': TimeFrame.Week,
            '1M': TimeFrame.Month
        }
        
        try:
            request_params = StockBarsRequest(
                symbol_or_symbols=[symbol],
                timeframe=step_dict[step],
                start=since,
                end=until,
                limit=10000  # Max allowed by Alpaca
            )
            
            bars = self.alpaca_client.get_stock_bars(request_params)
            if not bars:
                logging.warning(f"No bars returned for {symbol}")
                return pd.DataFrame()
                
            df = bars.df
            if df.empty:
                logging.warning(f"Empty DataFrame returned for {symbol}")
                
            return df
            
        except Exception as e:
            logging.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()

    def get_data(self, ticker: str = "AAPL", start_date: str = "earliest", end_date: str = "latest", step: str = "1h") -> pd.DataFrame:
        since, until = self.handle_time_boundaries(start_date, end_date)
        since_str = since.strftime("%Y-%m-%d")
        until_str = until.strftime("%Y-%m-%d")
        
        # Ensure we're not requesting future dates
        today = datetime.today().date()
        if until.date() > today:
            until = datetime.combine(today, datetime.min.time())
            logging.warning(f"Adjusted end_date to today ({today}) as future dates are not available")
        
        filename = f'{ticker.replace("/","-")}_{step}_{since_str}_{until_str}.csv'
        cached_df = self.check_cached_file(filename)
        if cached_df is not None:
            logging.info(f'Using cached data from {filename}')
            return cached_df
        else:
            try:
                df = self.fetch_alpaca_data(ticker, since, until, step)
                if df.empty:
                    logging.warning(f"No data found for {ticker} between {since_str} and {until_str}")
                    return pd.DataFrame()
                
                df = self.transform_raw_data(df)
                df = df.reset_index()
                df.rename(columns={'timestamp': "dt"}, inplace=True)
                df['dt'] = pd.to_datetime(df['dt'])
                self.save_to_file(df, filename)
                return df
            except Exception as e:
                logging.error(f"Error fetching data for {ticker}: {str(e)}")
                return pd.DataFrame()
        
class PolygonDataFetcher(BaseDataFetcher):
    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')

        if not self.api_key:
            raise ValueError('API key must be provided either as arguments or as environment variables.')
        
        # Separate mappings for timespan units and valid multipliers
        self.timespan_mapping = {
            'm': 'minute',
            'h': 'hour',
            'd': 'day',
            'w': 'week',
            'M': 'month'
        }
        
        self.valid_multipliers = {
            'minute': [1, 5, 15, 30],
            'hour': [1, 2, 4],
            'day': [1],
            'week': [1],
            'month': [1]
        }
    def get_markets(self) -> List:
        return super().get_markets()
    
    class PolygonRateLimitError(Exception):
        """Custom exception for Polygon API rate limits"""
        pass

    @retry(
        retry=retry_if_exception_type(PolygonRateLimitError),
        wait=wait_exponential(multiplier=2, min=4, max=60),
        stop=stop_after_attempt(5),
        after=lambda retry_state: logging.warning(
            f"Retry attempt {retry_state.attempt_number} for Polygon API request after {retry_state.outcome.exception()}. "
            f"Waiting {retry_state.next_action.sleep if retry_state.next_action else 'N/A'} seconds before next attempt"
        )
    )
    def _make_polygon_request(self, url: str) -> dict:
        """Make a request to Polygon API with retry logic"""
        response = requests.get(url)
        data = response.json()
        
        if data.get('status') != 'OK':
            error_msg = data.get('error', 'Unknown error')
            if 'maximum requests per minute' in error_msg:
                raise self.PolygonRateLimitError(f"Rate limit exceeded: {error_msg}")
            raise ValueError(f"Polygon API error: {error_msg}")
            
        return data

    def get_ticker(self) -> List[str]:
        """Fetch active stock tickers from Polygon API"""
        url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={self.api_key}"
        symbols = []
        
        while url:
            try:
                data = self._make_polygon_request(url)
                symbols.extend([ticker['ticker'] for ticker in data['results']])
                
                # Check for next page
                url = data.get('next_url')
                if url:
                    url = f"{url}&apiKey={self.api_key}"
                    
            except Exception as e:
                logging.error(f"Error fetching symbols from Polygon: {str(e)}")
                break
                
        return symbols

    def _parse_step(self, step: str) -> tuple[int, str]:
        """
        Convert step string to multiplier and timespan
        Example: '15m' -> (15, 'minute'), '2h' -> (2, 'hour')
        """
        import re
        
        # Extract multiplier and unit from step string
        match = re.match(r'(\d+)([mhdwM])', step)
        if not match:
            raise ValueError(f"Invalid step format: {step}. Example format: '15m', '2h', '1d'")
            
        multiplier = int(match.group(1))
        unit = match.group(2)
        
        if unit not in self.timespan_mapping:
            raise ValueError(f"Unsupported time unit: {unit}. Supported units are: {list(self.timespan_mapping.keys())}")
            
        timespan = self.timespan_mapping[unit]
        
        # Validate multiplier for the given timespan
        if multiplier not in self.valid_multipliers[timespan]:
            raise ValueError(
                f"Invalid multiplier {multiplier} for {timespan}. "
                f"Valid multipliers are: {self.valid_multipliers[timespan]}"
            )
            
        return multiplier, timespan

    def get_data(self, ticker: str = 'AAPL', start_date: Optional[str] = None, end_date: Optional[str] = None, 
                step: str = '1h', limit: int = 50_000) -> pd.DataFrame:
        """
        Fetches stock data for a given ticker within a date range from the Polygon API.

        :param ticker: The stock ticker symbol.
        :param start_date: The start date in 'YYYY-MM-DD' format.
        :param end_date: The end date in 'YYYY-MM-DD' format.
        :param multiplier: The size of the timespan multiplier (e.g., 1, 5, 15).
        :param timespan: The timespan (e.g., 'minute', 'hour', 'day').
        :param api_key: The API key for the Polygon API.
        :param limit: The number of results to fetch per request. Default is 120.
        :return: A DataFrame containing the aggregated stock data.
        """
        if not start_date:
            start_date = '1971-01-01'
        if not end_date:
            end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

        base_url = "https://api.polygon.io/v2/aggs/ticker"
        multiplier, timespan = self._parse_step(step)
        url = f"{base_url}/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}&apiKey={self.api_key}"
        filename = f'{ticker.replace("/","-")}_{step}_{start_date}_{end_date}.csv'
        cached_df = self.check_cached_file(filename)

        if cached_df is not None:
            print(f'cached {filename}')
            return cached_df
        else:
            df = pd.DataFrame()
            while url:
                try:
                    data = self._make_polygon_request(url)
                    current_page = pd.DataFrame(data['results'])
                    df = pd.concat([df, current_page], ignore_index=True)

                    next_url = data.get('next_url', None)
                    if next_url:
                        url = f"{next_url}&apiKey={self.api_key}"
                    else:
                        url = None
                except Exception as e:
                    logging.error(f"Error fetching data from Polygon: {str(e)}")
                    break

            df.rename(columns={'t': 'dt','c':'close','o':'open','h':'high','l':'low','v':'volume'}, inplace=True)
            df = self.transform_raw_data(df)
            # Only drop columns if they exist
            columns_to_drop = [col for col in ['vw', 'n'] if col in df.columns]
            if columns_to_drop:
                df.drop(columns=columns_to_drop, inplace=True)
            df['dt'] = pd.to_datetime(df['dt'], unit='ms')
            self.save_to_file(df, filename)

            return df
