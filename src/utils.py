import logging
import os
import sys

from typing import Optional
from datetime import datetime

from pathlib import Path

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(level: Optional[int] = logging.INFO,
                 stdout_log: bool = True,
                 file_log: bool = True):
    if not (stdout_log or file_log):
        exit(">>> stdout and file logs are False")

    handlers = []

    if file_log:
        log_filename: Path = Path(
            ROOT_DIR.parent / f"logs/logs_{datetime.now():%S-%m-%d-%Y}.log"
        ).resolve()

        os.makedirs(log_filename.parent, exist_ok=True)
        handlers.append(logging.FileHandler(log_filename))

    if stdout_log:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=level,
        format='%(module)s - %(lineno)d - %(levelname)s - %(message)s',
        handlers=handlers
    )

    if file_log:
        logging.info(f"Log ({file_log=}, {stdout_log=}) file was created at {log_filename}")
    else:
        logging.info(f"Log file wasn't created due to {file_log=}")

