import logging
import os
import sys
import json

import requests

from typing import Optional, Tuple, List
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import colorlog

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def setup_logger(level: Optional[int] = logging.NOTSET,
                 stdout_log: Optional[bool] = True,
                 file_log: Optional[bool] = True) -> None:

    if not (stdout_log or file_log):
        exit(">>> stdout and file logs are False")

    handlers = []
    log_filename = Path()

    if file_log:
        log_filename: Path = Path(
            ROOT_DIR.parent / f"logs/logs_{datetime.now():%S-%m-%d-%Y}.log"
        ).resolve()

        os.makedirs(log_filename.parent, exist_ok=True)
        handlers.append(logging.FileHandler(log_filename))

    if stdout_log:
        color_formatter = colorlog.ColoredFormatter(
            '%(log_color)s>>> %(module)s:%(lineno)d - %(levelname)s - %(message)s',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        stream_handler.setFormatter(color_formatter)
        handlers.append(stream_handler)

    logging.basicConfig(
        level=level,
        format='>>> %(module)s:%(lineno)d - %(levelname)s - %(message)s',
        handlers=handlers
    )

    if file_log:
        logging.info(f"Log ({file_log=}, {stdout_log=}) file was created at {log_filename}")
    else:
        logging.info(f"Log file wasn't created due to {file_log=}")


def setup_session(retries: Optional[int] = 5,
                  backoff_factor: Optional[float] = 0.3,
                  status_forcelist: Optional[Tuple[int, ...]] = (400, 403, 404, 500, 502, 504),
                  pool_maxsize: Optional[int] = 50) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        raise_on_status=False,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=pool_maxsize)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def read_json(file_path: str) -> Tuple[List[str], List[int]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        vacancies = data.get('vacancies', [])
        cities = data.get('cities', [])

        if not isinstance(vacancies, list) or not all(isinstance(v, str) for v in vacancies):
            raise ValueError("Invalid format for 'vacancies'. Expected a list of strings.")

        if not isinstance(cities, list) or not all(isinstance(c, int) for c in cities):
            raise ValueError("Invalid format for 'cities'. Expected a list of integers.")

        return vacancies, cities

    except FileNotFoundError:
        logging.error(f"File not found: {file_path}", exc_info=True)
        return [], []
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {file_path}", exc_info=True)
        return [], []
    except ValueError as e:
        logging.error(f"Value error: {e}", exc_info=True)
        return [], []
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        return [], []
