import os
import sys
import logging
from pathlib import Path
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import yaml

from initialize_logfile import initialize_logging

def check_for_missing_values(data, ticker):
    """Checks for missing values in the dataset."""
    try: 
        missing_values = data.isnull().sum() 
        total_missing = missing_values.sum() 

        if total_missing > 0:
            logging.warning(f"⚠️ Ticker {ticker} contains missing values and will be skipped.")
            return False

        logging.info(f"✅ The data of {ticker} has no missing values.")
        return True

    except Exception as e:
        logging.error(f"❌ Error checking missing values for {ticker}: {e}", exc_info=True)
        return False


def normalize_data(data, ticker):
    """Applies MinMaxScaler normalization to selected columns."""
    try:
        required_columns = ["Open", "High", "Low", "Close", "Volume"]
        missing_cols = [col for col in required_columns if col not in data.columns]

        if missing_cols:
            logging.error(f"❌ Missing columns in {ticker}: {missing_cols}, skipping normalization.")
            return None

        scaler = MinMaxScaler()
        data[required_columns] = data[required_columns].fillna(0)  # Handle NaNs
        data[required_columns] = scaler.fit_transform(data[required_columns])

        logging.info(f"✅ Data for {ticker} normalized successfully.")
        return data

    except Exception as e:
        logging.error(f"❌ Normalization failed for {ticker}: {e}", exc_info=True)
        return None


def convert_to_chronos_format(data, ticker):
    """Converts data into Chronos-compatible format."""
    try:
        chronos_data = data[["item_id", "Date", "Close", "Open", "High", "Low", "Volume"]]
        chronos_data.rename(columns={"Date": "timestamp"}, inplace=True)

        logging.info(f"✅ Data for {ticker} converted to Chronos format.")
        return chronos_data

    except Exception as e:
        logging.error(f"❌ Error converting {ticker} to Chronos format: {e}")
        return None


def saving_ts_chronos(data, ticker, path="./data/chronos"):
    """Saves the processed dataset to a CSV file."""
    try:
        os.makedirs(Path(path), exist_ok=True)
        file_path = os.path.join(path, f"{ticker}.csv")

        data.to_csv(file_path, index=False)
        logging.info(f"✅ Chronos data saved for {ticker} at {file_path}.")

    except Exception as e:
        logging.error(f"❌ Saving Chronos data for {ticker} failed. Error: {e}")


if __name__ == "__main__":
    script_name = os.path.splitext(os.path.basename(sys.argv[0] if "__file__" not in globals() else __file__))[0]
    initialize_logging(script_name)
    logging.info(f"Script: {script_name} started.")
    logging.info("Processing started.")

    # Ensure the output directory exists
    os.makedirs(Path("./data/chronos"), exist_ok=True)

    # List to store failed tickers
    failed_tickers = []

    try:
        # Load YAML configuration
        yaml_file = Path("./configuration/tickers.yaml")
        if not yaml_file.exists():
            logging.error(f"❌ YAML file {yaml_file} not found.")
            sys.exit(1)

        with yaml_file.open("r") as file:
            yaml_data = yaml.safe_load(file)

        # Extract all tickers
        all_tickers = []
        for category, tickers in yaml_data["tickers"].items():
            all_tickers.extend(tickers)

        # Process each ticker
        directory_raw_data = Path("./data/raw")
        if not directory_raw_data.exists():
            logging.error("❌ The directory '../data/raw' does not exist. Ensure the dataset is available.")
            sys.exit(1)

        for file_path in directory_raw_data.glob("*.csv"):
            if file_path.name == ".DS_Store":  # Skip system files
                continue

            try:
                data = pd.read_csv(file_path, encoding="utf-8", engine="python")
            except Exception as e:
                logging.error(f"❌ Failed to read {file_path}. Error: {e}")
                failed_tickers.append(file_path.stem)
                continue  # Skip to next file

            ticker = file_path.stem

            # Check for missing values before proceeding
            if not check_for_missing_values(data, ticker):
                failed_tickers.append(ticker)  # Add to failed list
                continue  # Skip this ticker

            # Normalize and convert data
            data_norm = normalize_data(data, ticker)
            if data_norm is None:
                failed_tickers.append(ticker)
                continue  # Skip this ticker

            data_chronos = convert_to_chronos_format(data_norm, ticker)
            if data_chronos is None:
                failed_tickers.append(ticker)
                continue  # Skip this ticker

            # Save processed data
            saving_ts_chronos(data_chronos, ticker)

        logging.info("✅ Processing completed successfully.")

        # Log all failed tickers at the end
        if failed_tickers:
            logging.warning(f"❌ The following tickers were skipped due to errors: {', '.join(failed_tickers)}")

    except Exception as e:
        logging.error(f"❌ The processing was unsuccessful. Error: {e}", exc_info=True)
