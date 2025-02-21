import os
import logging
from datetime import datetime

LOGS_DIR = "./logs"

def initialize_logging(script_name):
    log_dir = os.path.join(LOGS_DIR, script_name)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{datetime.today().strftime('%Y-%m-%d')}.log")
    
    logging.basicConfig(
        filename=log_file,
        filemode='w',
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    logging.info("Logging setup complete.")
