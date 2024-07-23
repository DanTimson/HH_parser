import src.config
import src.parser

import logging
from src.utils import setup_logger


def main():
    setup_logger(stdout_log=False)
    logging.info('Start pool')
    logging.info('Start')


if __name__ == '__main__':
    main()
