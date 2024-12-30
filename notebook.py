#%%
from data_fetcher import CryptoDataFetcher, AlpacaDataFetcher, PolygonDataFetcher
from dotenv import load_dotenv
load_dotenv()

fetchers = [CryptoDataFetcher(), AlpacaDataFetcher(), PolygonDataFetcher()]
for fetcher in fetchers:
    
    print(fetcher.get_markets().keys())
# %%
