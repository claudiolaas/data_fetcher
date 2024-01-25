import time
import requests
import ccxt
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
from alpaca.data.historical import StockHistoricalDataClient
from datetime import datetime, timedelta
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import json
from alpaca_trade_api.rest import REST
import os

from abc import ABC, abstractmethod

class BaseDataFetcher(ABC):

    @abstractmethod
    def get_data(self):
        pass

    @abstractmethod
    def get_markets(self):
        pass

    @abstractmethod
    def get_symbols(self):
        pass

    @abstractmethod
    def transform_raw_data(self):
        pass

    def save_to_file(self, df, filename):
        my_file = Path("csvs/") / filename
        if not Path("csvs/").is_dir():
            Path("csvs/").mkdir()
        df.to_csv(my_file, index=False)

    def check_cached_file(self, filename):
        my_file = Path("csvs/") / filename
        if my_file.is_file():
            return pd.read_csv(my_file)
        return None
    
    def transform_raw_data(self, df):
        
        df["log_return"] = np.log(df["close"]).diff().fillna(0)
        df["asset_return"] = df["close"] / df["close"].shift()
        df["asset_return"].fillna(1, inplace=True)
        return df


class CryptoDataFetcher(BaseDataFetcher):
    def __init__(self):
        self.exchange = ccxt.binance()

    def get_markets(self):
        markets = self.exchange.load_markets()
        return markets  
    
    def get_symbols(self):
        markets = self.exchange.load_markets()
        return list(markets.keys())
    
    def get_earliest(self, market):
        since = self.exchange.parse8601('2010-01-01' + "T00:00:00Z")
        until = self.exchange.parse8601('2050-01-01' + "T00:00:00Z")
        while since < until:
            orders = self.exchange.fetchOHLCV(market, timeframe='1M', since=since)
            if len(orders) > 0:
                earliest = orders[0][0]
                return earliest
            else:
                since += (1000 * 60 * 60 * 24 * 30) # shift forward 30 days

    def handle_time_boundaries(self, start, end, market):
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
    
    def fetch_exchange_data(self, market, timeframe, since, until):
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
    
    def get_data(self, start="earliest", end="latest", market="BTC/USDT", step="1h"):
        since, until = self.handle_time_boundaries(start, end, market)
        since_str = self.exchange.iso8601(since)[:10]
        until_str = self.exchange.iso8601(until)[:10]
        
        filename = f'{since_str}_{until_str}_{market.replace("/","-")}_{step}.csv'
        cached_df = self.check_cached_file(filename)
        if cached_df is not None:
            print(f'cached {filename}')
            return cached_df
        else:
            df_list = self.fetch_exchange_data(market, step, since, until)
            df = pd.DataFrame(df_list).reset_index()
            df.rename(columns={0: "milliseconds", 1: "open", 2: "high", 3: "low", 4: "close", 5: "volume"}, inplace=True)
            df["dt"] = df["milliseconds"].apply(lambda x: self.exchange.iso8601(x))
            df = self.transform_raw_data(df)
            self.save_to_file(df, filename)
            return df
        


class AlpacaDataFetcher(BaseDataFetcher):
    def __init__(self, api_key=None, secret_key=None):
        self.api_key = api_key or os.getenv('ALPACA_API_KEY')
        self.secret_key = secret_key or os.getenv('ALPACA_SECRET_KEY')

        if not self.api_key or not self.secret_key:
            raise ValueError('API key and secret key must be provided either as arguments or as environment variables.')

        self.alpaca_client = StockHistoricalDataClient(api_key=self.api_key, secret_key=self.secret_key)
        self.alpaca_rest = REST(self.api_key, self.secret_key) 
        
    def get_symbols(self):
        assets =  self.alpaca_rest.list_assets()
        return [asset.symbol for asset in assets]
    
    def get_markets(self):
        return self.alpaca_rest.list_assets()
    
    def get_earliest(self, market):
        since = self.exchange.parse8601('2010-01-01' + "T00:00:00Z")
        until = self.exchange.parse8601('2050-01-01' + "T00:00:00Z")
        while since < until:
            orders = self.exchange.fetchOHLCV(market, timeframe='1M', since=since)
            if len(orders) > 0:
                earliest = orders[0][0]
                return earliest
            else:
                since += (1000 * 60 * 60 * 24 * 30)

    def handle_time_boundaries(self, start, end):
        earliest = datetime.strptime("1800-01-01", '%Y-%m-%d')
        latest = datetime.today() - timedelta(days=1)
        since = earliest if start == "earliest" else datetime.strptime(start, '%Y-%m-%d')
        until = latest if end == "latest" else datetime.strptime(end, '%Y-%m-%d')
        return since, until
    
    def fetch_alpaca_data(self, symbol, since, until,step):
        step_dict = {
            '1m': TimeFrame.Minute,
            '1h': TimeFrame.Hour,
            '1d': TimeFrame.Day,
            '1w': TimeFrame.Week,
            '1M': TimeFrame.Month
        }
        request_params = StockBarsRequest(
            symbol_or_symbols=[symbol],
            timeframe=step_dict[step],  
            start=since,
            end=until
        )
        bars = self.alpaca_client.get_stock_bars(request_params)
        return bars.df

    def get_data(self, start="earliest", end="latest", market="BTC/USDT", step="1h"):
        since, until = self.handle_time_boundaries(start, end)
        since_str = since.strftime("%Y-%m-%d")
        until_str = until.strftime("%Y-%m-%d")
        
        filename = f'{since_str}_{until_str}_{market.replace("/","-")}_{step}.csv'
        cached_df = self.check_cached_file(filename)
        if cached_df is not None:
            print(f'cached {filename}')
            return cached_df
        else:
            df = self.fetch_alpaca_data(market, since, until,step)
            df = self.transform_raw_data(df)
            df.rename(columns={'timestamp': "dt"}, inplace=True)
            df['dt'] = pd.to_datetime(df['dt'], unit='ms')
            self.save_to_file(df, filename)
            df.dro
            return df
        
class PolygonDataFetcher(BaseDataFetcher):
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')

        if not self.api_key:
            raise ValueError('API key must be provided either as arguments or as environment variables.')
    def get_markets(self):
        return super().get_markets()
    
    def get_symbols(self):
        return super().get_symbols()

    def get_data(self, ticker='AAPL', start_date=None, end_date=None, multiplier=1, timespan='hour', limit=50_000):
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
        url = f"{base_url}/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?adjusted=true&sort=asc&limit={limit}&apiKey={self.api_key}"
        filename = f'{start_date}_{end_date}_{ticker.replace("/","-")}_{multiplier}{timespan}.csv'
        cached_df = self.check_cached_file(filename)

        if cached_df is not None:
            print(f'cached {filename}')
            return cached_df
        else:
            df = pd.DataFrame()
            while url:
                print(url)
                response = requests.get(url)
                data = response.json()
                
                if data['status'] != 'OK':
                    # Check for rate limit error
                    if 'maximum requests per minute' in data.get('error', ''):
                        print("Rate limit exceeded. Waiting for 60 seconds before retrying...")
                        time.sleep(60)  # Wait for 60 seconds
                        continue  # Retry the request
                    else:
                        print(f"Error: {data['status']} - {data.get('error', 'Unknown error')}")
                        break


                current_page = pd.DataFrame(data['results'])
                df = pd.concat([df, current_page], ignore_index=True)

                next_url = data.get('next_url', None)
                if next_url:
                    url = f"{next_url}&apiKey={self.api_key}"
                else:
                    url = None

            df.rename(columns={'t': 'dt','c':'close','o':'open','h':'high','l':'low','v':'volume'}, inplace=True)
            df = self.transform_raw_data(df)
            df.drop(columns=['vw','n'], inplace=True)
            df['dt'] = pd.to_datetime(df['dt'], unit='ms')
            self.save_to_file(df, filename)

            return df
