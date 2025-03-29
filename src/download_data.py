"""
    Initial download of the data from yFinance.
"""

import yfinance as yf
import pandas as pd
import os
import yaml
import logging
from pathlib import Path
import sys

from initialize_logfile import initialize_logging

# PARAMETERS FOR YFINANCE
INTERVAL = "1wk"
PERIODE = "730d"

RELEVANT_COLUMNS = ["Date", "Close", "Volume"]

YAML_FILE = "./configuration/tickers.yaml"             
RAW_DATA_DIR = "./data/raw"  

def fetch_ts(ticker, interval=INTERVAL, period=PERIODE):
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(period=period, interval=interval)

        if data.empty:
            logging.warning(f"❌ Ticker {ticker} not found or has no data, and will not be included.")
            return None

        # Reset index to get Date as a column
        data.reset_index(inplace=True)

        # Select relevant columns (the other columns are zero)
        data = data[RELEVANT_COLUMNS]
        data.rename(columns={"Datetime": "Date"}, inplace=True)

        # Removing time-zone offset
        data["Date"] = pd.to_datetime(data["Date"]).dt.tz_localize(None)

        # Each ts must have an item ID to be able to train on the Chronos model
        data["item_id"] = ticker
        logging.info(f"✅ Successfully extracted data for {ticker}.")

        return data

    except Exception as e:
        logging.error(f"❌ An unexpected error occurred while fetching {ticker}: {e}")
        return None

def saving_ts_raw(data, ticker, path=RAW_DATA_DIR):
    try:
        os.makedirs(Path(path), exist_ok=True)
        file_path = os.path.join(path, f"{ticker}.parquet")

        data.to_parquet(file_path, index=False)
        logging.info(f"✅ Data saved for {ticker} at {file_path}.")

    except Exception as e:
        logging.error(f"❌ Saving data for {ticker} was unsuccessful. Error: {e}")

def main():
    script_name = os.path.splitext(os.path.basename(sys.argv[0] if "__file__" not in globals() else __file__))[0]
    initialize_logging(script_name)
    logging.info(f"Script: {script_name}.py started.")
    logging.info("Downloading started.")

    os.makedirs(Path(RAW_DATA_DIR), exist_ok=True)

    failed_tickers = []

    try:
        with open(YAML_FILE, "r") as file:
            yaml_data = yaml.safe_load(file)

        all_tickers = []
        for category, tickers in yaml_data["tickers"].items():
            all_tickers.extend(tickers)

        for ticker in all_tickers:
            data = fetch_ts(ticker)
            if data is None:
                failed_tickers.append(ticker)
                continue  # Skip saving for failed tickers

            saving_ts_raw(data, ticker)

        logging.info("✅ Downloading the data successfully completed.")

        if failed_tickers:
            logging.warning(f"❌ The following tickers could not be processed: {', '.join(failed_tickers)}")

    except Exception as e:
        logging.error(f"❌ Downloading the dataset was unsuccessful. Error: {e}")

if __name__ == "__main__":
    main()

