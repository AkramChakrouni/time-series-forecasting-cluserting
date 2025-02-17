import os
import sys
import logging
from datetime import datetime

def initialize_logging(script_name):
    # Setting up the log file, this does not work in Jupyter notebooks
    log_dir = f"./logs/{script_name}"
    log_file = f"{log_dir}/{datetime.today().strftime('%Y-%m-%d')}.log"
    os.makedirs(log_dir, exist_ok=True)

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    logging.info("Logging setup complete.")