#%%
from data_fetcher import CryptoDataFetcher, AlpacaDataFetcher, PolygonDataFetcher
from dotenv import load_dotenv
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def main():
    load_dotenv()
    
    fetchers = [
        # CryptoDataFetcher(),
        AlpacaDataFetcher(),
        # PolygonDataFetcher()
    ]
    
    for fetcher in fetchers:
        try:
            logging.info(f"Fetching markets using {fetcher.__class__.__name__}")
            symbols = fetcher.get_ticker()
            if symbols:
                logging.info(f"Fetched {len(symbols)} symbols. First 5: {list(symbols)[:5]}...")
                
                # Fetch hourly data for first symbol on Dec 29, 2024
                first_symbol = 'AAPL'#list(symbols)[0]
                try:
                    logging.info(f"Fetching hourly data for {first_symbol} on 2024-12-29")
                    data = fetcher.get_data(
                        start_date="2023-12-28",
                        end_date="2024-12-29",
                        ticker=first_symbol,
                        step="1h"
                    )
                    logging.info(f"Data fetched:\n{data.head()}")
                except Exception as e:
                    logging.error(f"Error fetching data for {first_symbol}: {str(e)}")
            else:
                logging.warning("No markets returned")
        except Exception as e:
            logging.error(f"Error fetching markets with {fetcher.__class__.__name__}: {str(e)}")
            logging.debug("Full error details:", exc_info=True)

if __name__ == "__main__":
    main()
