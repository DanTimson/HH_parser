from src.parser import collect_vacancies_data

import logging
from src.utils import setup_logger


def main():
    setup_logger(stdout_log=True, file_log=False, level=logging.INFO)

    cities = [1, 2]
    vacancies = ['Data Scientist', 'Software Engineer']
    result = collect_vacancies_data(cities, vacancies)
    print(result['url'])


if __name__ == '__main__':
    main()
