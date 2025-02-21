#CHECK FOR CORRECTNESS
import os
import yaml
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import yfinance as yf

from initialize_logfile import initialize_logging

YAML_FILE = "./configuration/tickers.yaml"          
DATA_DIR = "./data/raw"            
DEFAULT_START_DATE = "2022-01-01"     


def load_stocks_from_yaml(yaml_file):
    """
    Loads the list of stock tickers from the YAML file.
    Expects the YAML file to have a key "stocks" with a list of tickers.
    """
    try:
        with open(yaml_file, "r") as f:
            config = yaml.safe_load(f)
        stocks = config.get("stocks", [])
        logging.info(f"Loaded {len(stocks)} stocks from {yaml_file}.")
        return stocks
    except Exception as e:
        logging.error(f"Error loading YAML file {yaml_file}: {e}")
        raise


def fetch_stock_data(ticker, start_date):
    """
    Fetches stock data for a given ticker from 'start_date' until today using yfinance.
    Returns a DataFrame with the data.
    """
    try:
        end_date = datetime.today().strftime("%Y-%m-%d")
        logging.info(f"Fetching data for {ticker} from {start_date} to {end_date}.")
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if data.empty:
            logging.warning(f"No new data fetched for {ticker} from {start_date}.")
        else:
            logging.info(f"Fetched {len(data)} rows for {ticker}.")
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()


def update_stock_data(ticker, data_dir):
    """
    Updates (or creates) the stock data file for a given ticker.
    - If a file exists: reads it, finds the latest date, downloads new data starting the next day,
      appends it, removes duplicate rows, sorts by date, and saves the updated data.
    - If no file exists: downloads full history from DEFAULT_START_DATE and saves it.
    """
    file_path = Path(data_dir) / f"{ticker}.parquet"
    
    if file_path.exists():
        try:
            existing_data = pd.read_parquet(file_path)
            # Ensure the DataFrame has a DatetimeIndex. If not, try to convert a 'Date' column.
            if not isinstance(existing_data.index, pd.DatetimeIndex):
                if "Date" in existing_data.columns:
                    existing_data["Date"] = pd.to_datetime(existing_data["Date"])
                    existing_data.set_index("Date", inplace=True)
                else:
                    existing_data.index = pd.to_datetime(existing_data.index)
                    
            last_date = existing_data.index.max()
            # Start fetching from the day after the last date to avoid duplicates
            new_start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            logging.info(f"{ticker}: Last date in existing data is {last_date.date()}, fetching from {new_start_date}.")
            new_data = fetch_stock_data(ticker, new_start_date)
            
            if not new_data.empty:
                combined = pd.concat([existing_data, new_data])
                combined = combined[~combined.index.duplicated(keep='last')]
                combined.sort_index(inplace=True)
                combined.to_parquet(file_path)
                logging.info(f"Updated {ticker}: now {len(combined)} rows of data.")
            else:
                logging.info(f"No new data available for {ticker}.")
        except Exception as e:
            logging.error(f"Error updating data for {ticker}: {e}")
    else:
        logging.info(f"No existing data for {ticker}. Fetching from {DEFAULT_START_DATE}.")
        data = fetch_stock_data(ticker, DEFAULT_START_DATE)
        if not data.empty:
            data.to_parquet(file_path)
            logging.info(f"Saved new data for {ticker} with {len(data)} rows.")
        else:
            logging.warning(f"❌ No data fetched for {ticker}.")


def remove_stocks_not_in_list(valid_stocks, data_dir):
    """
    Removes any stock data files in the data directory whose tickers are not in the valid_stocks list.
    """
    data_dir = Path(data_dir)
    for file in data_dir.glob("*.parquet"):
        ticker = file.stem
        if ticker not in valid_stocks:
            try:
                os.remove(file)
                logging.info(f"Removed data file for {ticker} (no longer in YAML list).")
            except Exception as e:
                logging.error(f"❌ Error removing file {file}: {e}")

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    stocks = load_stocks_from_yaml(YAML_FILE)
    
    for ticker in stocks:
        update_stock_data(ticker, DATA_DIR)
    
    remove_stocks_not_in_list(stocks, DATA_DIR)
    
    logging.info("✅ Data update completed successfully.")

if __name__ == "__main__":
    main()
