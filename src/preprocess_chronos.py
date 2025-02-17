import os
import logging
from pathlib import Path

from initialize_logfile import initialize_logging

if __name__ == "__main__":
    script_name = os.path.splitext(os.path.basename(sys.argv[0] if "__file__" not in globals() else __file__))[0]
    initialize_logging(script_name)
    logging.info(f"Script: {script_name} started.")

    os.makedirs(Path("./data/chronos"), exist_ok=True)