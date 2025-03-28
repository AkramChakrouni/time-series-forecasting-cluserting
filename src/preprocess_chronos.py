"""
    Procressing the data from yFinance into suitable format for Amazon Chronos.
"""

import os
import logging
from pathlib import Path
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import sys
from autogluon.timeseries import TimeSeriesDataFrame

from initialize_logfile import initialize_logging

YAML_FILE = "./configuration/tickers.yaml"          
CHRONOS_DATA_DIR = "./data/processed"   
RAW_DATA_DIR = "./data/raw"         

def check_for_missing_values(data, ticker):
    try:
        missing_values = data.isnull().sum()
        total_missing = missing_values.sum()

        if total_missing > 0:
            logging.warning(f"❌ The data for {ticker} contains missing values and requires further processing before it can be added to the chronos directory.")
            return False
        else:
            logging.info(f"✅ The data for {ticker} has no missing values.")
            return True

    except ValueError as e:
        logging.error(f"❌ An error occurred while checking missing values for {ticker}: {e}", exc_info=True)
        return False

def normalize_data(data, ticker):
    try:
        # Ensure required columns exist
        required_columns = ["Date", "Close", "Volume"]
        missing_cols = [col for col in required_columns if col not in data.columns]
        if missing_cols:
            logging.error(f"❌ Missing columns in {ticker}: {missing_cols}")
            return None

        # Normalize only the 'Close' column (its name remains "Close")
        scaler = MinMaxScaler()
        data["Close"] = scaler.fit_transform(data[["Close"]])
        
        # Drop unwanted columns: Open, High, Low
        data = data.drop(columns=["Open", "High", "Low"], errors="ignore")

        logging.info(f"✅ Data for {ticker} normalized successfully.")
        return data

    except Exception as e:
        logging.error(f"❌ Normalization failed for {ticker}: {e}", exc_info=True)
        return None

def convert_to_chronos_format(data, ticker):
    try:
        # Ensure 'item_id' exists; if not, add it
        if "item_id" not in data.columns:
            data["item_id"] = ticker

        # Rename 'Date' to 'timestamp'; keep "Close" and "Volume" unchanged.
        chronos_data = data.copy()
        chronos_data.rename(columns={"Date": "timestamp"}, inplace=True)

        # Reorder columns to: item_id, timestamp, Close, Volume
        chronos_data = chronos_data[["item_id", "timestamp", "Close", "Volume"]]

        # Convert to a TimeSeriesDataFrame
        ts_data = TimeSeriesDataFrame(chronos_data)

        logging.info(f"✅ The data for {ticker} successfully converted to Chronos format.")
        return ts_data

    except Exception as e:
        logging.error(f"❌ An error occurred when converting the data for {ticker} to Chronos format: {e}", exc_info=True)
        return None

def saving_ts_chronos(ts_data, ticker, path):
    try:
        os.makedirs(Path(path), exist_ok=True)
        file_path = os.path.join(path, f"{ticker}.parquet")
        
        # Convert the processed data to a TimeSeriesDataFrame (if not already)
        chronos_final = TimeSeriesDataFrame(ts_data)
        # Reset index so that 'item_id' and 'timestamp' become explicit columns
        chronos_final = chronos_final.reset_index()
        chronos_final.to_parquet(file_path, index=False)
        
        logging.info(f"✅ Chronos data saved for {ticker} at {file_path}.")
    except Exception as e:
        logging.error(f"❌ Saving Chronos data for {ticker} was unsuccessful. Error: {e}")

def main():
    script_name = os.path.splitext(os.path.basename(sys.argv[0] if "__file__" not in globals() else __file__))[0]
    initialize_logging(script_name)
    logging.info(f"Script: {script_name}.py started.")
    logging.info("Starting data preprocessing.")

    os.makedirs(Path(CHRONOS_DATA_DIR), exist_ok=True)
    directory_raw_data = Path(RAW_DATA_DIR)
    if not directory_raw_data.exists():
        logging.error(f"❌ {directory_raw_data} directory does not exist.")
        raise Exception(f"{directory_raw_data} directory does not exist.")
    if not any(directory_raw_data.iterdir()):
        logging.error(f"❌ {directory_raw_data} directory is empty.")
        raise Exception(f"{directory_raw_data} is empty.")

    for file_path in directory_raw_data.glob("*.parquet"):
        if file_path.name == ".DS_Store":  # Skip system files
            continue

        data = pd.read_parquet(file_path)
        ticker = file_path.stem

        if check_for_missing_values(data, ticker):
            data_norm = normalize_data(data, ticker)
            if data_norm is not None:
                ts_data = convert_to_chronos_format(data_norm, ticker)
                if ts_data is not None:
                    saving_ts_chronos(ts_data, ticker, path=CHRONOS_DATA_DIR)
        else:
            logging.warning(f"❌ The data for {ticker} has missing values.")

    logging.info("✅ Saving the Chronos compatible data successfully completed.")

if __name__ == "__main__":
    main()
