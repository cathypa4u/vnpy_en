"""
Layer 1: Alpha Factory - Step 1: Lab and Data Setup

Initializes the AlphaLab environment and downloads all necessary market data.
This script should be run first.
"""
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from pykrx import stock
import traceback

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import HistoryRequest
from vnpy.alpha.lab import AlphaLab
from vnpy_kis.kis_datafeed import KisDatafeed

# --- Configuration ---
LAB_NAME = "kospi200_adaptive"
INDEX_SYMBOL = "1028.KRX"
START_DATE = datetime.now() - timedelta(days=365 * 10)  # 10 years of data
END_DATE = datetime.now()
HOURLY_DATA_YEARS = 3

def setup_lab_and_download_data():
    """
    Initializes AlphaLab and downloads historical data for KOSPI 200 components.
    """
    print("--- Alpha Factory: Step 1: Lab and Data Setup ---")
    
    # 1. Initialize AlphaLab
    lab = AlphaLab(LAB_NAME)
    print(f"AlphaLab initialized at: ./{LAB_NAME}/")

    # 2. Get historical index components using pykrx
    index_ticker, exchange_str = INDEX_SYMBOL.split(".")
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='M')
    
    index_components = {}
    print("Fetching historical index components (monthly)...")
    for dt in tqdm(dates):
        date_str = dt.strftime("%Y%m%d")
        try:
            tickers = stock.get_index_portfolio_deposit_file(index_ticker, date=date_str)
            if tickers:
                vt_symbols = [f"{t}.{exchange_str}" for t in tickers]
                index_components[dt.strftime("%Y-%m-%d")] = vt_symbols
        except Exception:
            print(f"Could not fetch component data for {date_str}, skipping.")
            
    # Save component data using AlphaLab
    lab.save_component_data(INDEX_SYMBOL, index_components)
    print(f"Saved component data for {len(index_components)} dates.")

    # 3. Download bar data for all unique symbols
    all_symbols = lab.load_component_symbols(INDEX_SYMBOL, START_DATE, END_DATE)
    all_symbols.add(INDEX_SYMBOL) # Also download data for the index itself
    
    print(f"Found {len(all_symbols)} unique symbols. Starting data download...")
    datafeed = KisDatafeed()
    
    for vt_symbol in tqdm(all_symbols, desc="Downloading Bar Data"):
        symbol, exchange_str = vt_symbol.split(".")
        exchange = Exchange(exchange_str)

        # Download Daily Bars (10 years)
        daily_bars = datafeed.query_bar_history(
            HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                start=START_DATE,
                end=END_DATE,
                interval=Interval.DAILY
            )
        )
        if daily_bars:
            lab.save_bar_data(daily_bars)

        # Download Hourly Bars (3 years)
        start_hourly = END_DATE - timedelta(days=365 * HOURLY_DATA_YEARS)
        hourly_bars = datafeed.query_bar_history(
            HistoryRequest(
                symbol=symbol,
                exchange=exchange,
                start=start_hourly,
                end=end_date,
                interval=Interval.HOUR
            )
        )
        if hourly_bars:
            lab.save_bar_data(hourly_bars)

    print("Data download and setup complete.")
    print("Next step: Run `dataset_generator.py` to create features.")

if __name__ == "__main__":
    setup_lab_and_download_data()
