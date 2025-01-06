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
        # AlpacaDataFetcher(),
        PolygonDataFetcher()
    ]
    
    for fetcher in fetchers:
        try:
            logging.info(f"Fetching markets using {fetcher.__class__.__name__}")
            symbols = fetcher.get_ticker()
            if symbols:
                logging.info(f"Fetched {len(symbols)} symbols. First 5: {list(symbols)[:5]}...")
                
                # Fetch hourly data for first symbol on Dec 29, 2024
                # first_symbol = list(symbols)[0]
                first_three = list(symbols)[::200]

                for sym in first_three:
                    try:
                        logging.info(f"Fetching hourly data for {sym} on 2024-12-29")
                        data = fetcher.get_data(
                            # start_date="2023-12-28",
                            # end_date="2024-12-29",
                            ticker=sym,
                            step="1h"
                        )
                        logging.info(f"Data fetched:\n{data.head()}")
                    except Exception as e:
                        logging.error(f"Error fetching data for {sym}: {str(e)}")
            else:
                logging.warning("No markets returned")
        except Exception as e:
            logging.error(f"Error fetching markets with {fetcher.__class__.__name__}: {str(e)}")
            logging.debug("Full error details:", exc_info=True)

if __name__ == "__main__":
    main()

# %%

# Add a script that loops over the csvs dir, reads the csv files, appends the column log_return with .cumsum() to a new dataframe and plots the reuslt using plotly  AI!