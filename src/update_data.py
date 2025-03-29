"""
    Updates the data for when e.g. some tickers are removed from the configuration file.
"""

import yfinance as yf
import pandas as pd
import os
import yaml
import logging
from pathlib import Path
from datetime import datetime, timedelta
import sys
import time

from initialize_logfile import initialize_logging
import preprocess_chronos

YAML_FILE = "./configuration/tickers.yaml"
RAW_DATA_DIR = "./data/raw"
INTERVAL = "1wk"

DEFAULT_START_DATE = (datetime.today() - timedelta(days=730)).strftime("%Y-%m-%d")
RELEVANT_COLUMNS = ["Date", "Close", "Volume"]

# Attempt to import YFRateLimitError for handling rate limiting
try:
    from yfinance.shared import YFRateLimitError
except ImportError:
    YFRateLimitError = Exception

def load_stocks_from_yaml(yaml_file):
    """
    Loads the list of stock tickers from the YAML file.
    Expects the YAML file to have a key "tickers" with subcategories containing lists of tickers.
    """
    try:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)
        tickers_dict = config.get("tickers", {})
        stocks = []
        for category, tickers in tickers_dict.items():
            stocks.extend(tickers)
        logging.info(f"✅ Loaded {len(stocks)} stocks from {yaml_file}.")
        return stocks
    except Exception as e:
        logging.error(f"❌ Error loading YAML file {yaml_file}: {e}", exc_info=True)
        raise

def fetch_ts(ticker, start_date=None, interval=INTERVAL, period=DEFAULT_START_DATE):
    """
    Fetches time series data for the given ticker.
    If start_date is provided, downloads data from that date onward; otherwise, downloads using the given period.
    Returns a DataFrame with the relevant columns, flat column names, and a "Date" column.
    """
    try:
        stock = yf.Ticker(ticker)
        if start_date:
            data = stock.history(start=start_date, interval=interval)
        else:
            data = stock.history(period=DEFAULT_START_DATE, interval=interval)
        if data.empty:
            logging.warning(f"⛔️ No new data fetched for {ticker} starting {start_date if start_date else period}.")
            return None
        data.reset_index(inplace=True)
        # Flatten MultiIndex columns if present
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        data = data[RELEVANT_COLUMNS]
        data.rename(columns={"Datetime": "Date"}, inplace=True)
        data["Date"] = pd.to_datetime(data["Date"]).dt.tz_localize(None)
        data["item_id"] = ticker
        logging.info(f"✅ Successfully fetched data for {ticker}.")
        return data
    except YFRateLimitError as e:
        logging.error(f"❌ Rate limit error for {ticker}: {e}. Retrying in 60 seconds.", exc_info=True)
        time.sleep(60)
        return fetch_ts(ticker, start_date, interval, period)
    except Exception as e:
        logging.error(f"❌ Error fetching data for {ticker}: {e}", exc_info=True)
        return None

def update_stock_data(ticker, data_dir):
    """
    Updates or creates the stock data file for a given ticker.
    - If a file exists: reads it, determines the last timestamp, fetches new data from just after that timestamp,
      appends the new data to the existing data, removes duplicate timestamps, sorts the data, resets the index so "Date" remains a column, and saves the file.
    - If no file exists: downloads data starting from DEFAULT_START_DATE and saves it.
    """
    file_path = Path(data_dir) / f"{ticker}.parquet"
    
    if file_path.exists():
        try:
            existing_data = pd.read_parquet(file_path)
            # Ensure "Date" is a column
            if "Date" not in existing_data.columns:
                existing_data = existing_data.reset_index()
            existing_data["Date"] = pd.to_datetime(existing_data["Date"])
            last_date = existing_data["Date"].max()
            # Use last_date + 1 second; then format as "%Y-%m-%d" (yfinance expects "YYYY-MM-DD")
            new_start_date = (last_date + timedelta(seconds=1)).strftime("%Y-%m-%d")
            logging.info(f"{ticker}: Last date in existing data is {last_date}. Fetching new data from {new_start_date}.")
            
            new_data = fetch_ts(ticker, start_date=new_start_date)
            if new_data is not None and not new_data.empty:
                combined = pd.concat([existing_data, new_data])
                combined.drop_duplicates(subset=["Date"], keep='last', inplace=True)
                combined.sort_values(by="Date", inplace=True)
                # Reset the index so that "Date" remains a column
                combined = combined.reset_index(drop=True)
                combined.to_parquet(file_path, index=False)
                logging.info(f"Updated {ticker}: now {len(combined)} rows.")
            else:
                logging.info(f"No new data available for {ticker}.")
        except Exception as e:
            logging.error(f"❌ Error updating data for {ticker}: {e}", exc_info=True)
    else:
        logging.info(f"⛔️ No existing data for {ticker}. Fetching full data from {DEFAULT_START_DATE}.")
        data = fetch_ts(ticker, start_date=DEFAULT_START_DATE)
        if data is not None and not data.empty:
            data.to_parquet(file_path, index=False)
            logging.info(f"✅ Saved new data for {ticker} with {len(data)} rows.")
        else:
            logging.warning(f"⛔️ No data fetched for {ticker}.")

def remove_stocks_not_in_list(valid_stocks, data_dir):
    """
    Removes any stock data files in the data directory whose tickers are not in the valid_stocks list.
    """
    if not valid_stocks:
        logging.warning("⛔️ No valid stocks found in YAML. Skipping removal of files.")
        return

    data_dir = Path(data_dir)
    for file in data_dir.glob("*.parquet"):
        ticker = file.stem
        if ticker not in valid_stocks:
            try:
                os.remove(file)
                logging.info(f"✅ Removed data file for {ticker} (not in YAML).")
            except Exception as e:
                logging.error(f"❌ Error removing file {file}: {e}", exc_info=True)

def main():
    script_name = os.path.splitext(os.path.basename(sys.argv[0] if "__file__" not in globals() else __file__))[0]
    initialize_logging(script_name)
    logging.info(f"Script: {script_name}.py started. Updating raw data files.")
    
    os.makedirs(Path(RAW_DATA_DIR), exist_ok=True)
    
    try:
        all_tickers = load_stocks_from_yaml(YAML_FILE)
    except Exception as e:
        logging.error("❌ Error loading tickers from YAML.", exc_info=True)
        return
    
    for ticker in all_tickers:
        update_stock_data(ticker, RAW_DATA_DIR)
    
    remove_stocks_not_in_list(all_tickers, RAW_DATA_DIR)

    preprocess_chronos.main()

    logging.info("✅ Data update completed successfully.")

if __name__ == "__main__":
    main()
