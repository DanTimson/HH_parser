import src.config
from src.parser import collect_vacancies_data

import logging
from src.utils import setup_logger


def main():
    setup_logger(stdout_log=True, file_log=False)

    cities = [1, 2]
    vacancies = ['Data Scientist', 'Software Engineer']
    result = collect_vacancies_data(cities, vacancies)
    print(result)


if __name__ == '__main__':
    main()
