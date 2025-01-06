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
                # first_symbol = list(symbols)[0]
                first_three = list(symbols)[::200]
                print(len(first_three))

                for sym in first_three:
                    try:
                        logging.info(f"Fetching hourly data for {sym} on 2024-12-29")
                        data = fetcher.get_data(
                            # start_date="2023-12-28",
                            # end_date="2024-12-29",
                            ticker=sym,
                            step="1d"
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
import os
import pandas as pd
import plotly.graph_objects as go
from glob import glob
import plotly.io as pio                                                                                                                                                                                                                                                                                                                                
pio.renderers.default = "browser"  
def plot_cumulative_returns():
    # Get all CSV files in the csvs directory
    csv_files = glob('csvs/*.csv')
    
    if not csv_files:
        logging.warning("No CSV files found in csvs directory")
        return
        
    # Create empty DataFrame to store results
    combined_returns = pd.DataFrame()
    
    # Process each CSV file
    for file in csv_files:
        try:
            # Extract symbol name from filename
            symbol = os.path.splitext(os.path.basename(file))[0]
            
            # Read CSV file
            df = pd.read_csv(file)
            
            
            # Calculate cumulative returns
            cum_returns = df['asset_return'].cumprod()
            
            # Add to combined DataFrame
            combined_returns[symbol] = cum_returns
            
            logging.info(f"Processed {symbol}")
        except Exception as e:
            logging.error(f"Error processing {file}: {str(e)}")
    
    # Create Plotly figure
    fig = go.Figure()
    
    # Add traces for each symbol
    for column in combined_returns.columns:
        fig.add_trace(
            go.Scatter(
                y=combined_returns[column],
                name=column,
                mode='lines'
            )
        )
    
    # Update layout
    fig.update_layout(
        title='Cumulative Log Returns by Symbol',
        yaxis_title='Cumulative Log Return',
        xaxis_title='Time Period',
        showlegend=True
    )
    
    # Show the plot
    fig.show()

# Run the plotting function
plot_cumulative_returns()

# %%
