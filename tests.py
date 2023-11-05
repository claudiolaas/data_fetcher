from data_fetcher import DataFetcher, CryptoDataFetcher, AlpacaDataFetcher

data_fetcher = CryptoDataFetcher()

df = data_fetcher.get_data(start="2021-01-01", end="2021-01-03", market="BTC/USDT", step="1h")
print(df.head())