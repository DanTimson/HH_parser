import logging
import os

from typing import Optional
from datetime import datetime

from pathlib import Path

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(level: Optional[int] = logging.INFO,
                 stdout_log: bool = True):

    log_filename: Path = Path(
        ROOT_DIR.parent / f"logs/logs_{datetime.now():%S-%m-%d-%Y}.log"
    ).resolve()

    handlers = [logging.FileHandler(log_filename), logging.StreamHandler()]
    if not stdout_log:
        handlers.pop(-1)

    os.makedirs(log_filename.parent, exist_ok=True)
    logging.basicConfig(level=level,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=handlers
    )
    logging.info(f"Log file was created at {log_filename}")
