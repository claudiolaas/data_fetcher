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
        # AlpacaDataFetcher(),
        PolygonDataFetcher()
    ]
    
    for fetcher in fetchers:
        try:
            logging.info(f"Fetching markets using {fetcher.__class__.__name__}")
            symbols = fetcher.get_symbols()
            if symbols:
                logging.info(f"Markets fetched successfully: {list(symbols)[:5]}...")  # Show first 5 keys
            else:
                logging.warning("No markets returned")
        except Exception as e:
            logging.error(f"Error fetching markets with {fetcher.__class__.__name__}: {str(e)}")
            logging.debug("Full error details:", exc_info=True)

if __name__ == "__main__":
    main()
x