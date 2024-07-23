from src.parser import collect_vacancies_data

import logging
from src.utils import setup_logger, read_json


def main():
    setup_logger(stdout_log=True, file_log=False, level=logging.INFO)

    vacancies_json, cities_json = read_json('test.json')

    cities = [
        19, 22, 25, 26, 33, 41, 43, 47, 50, 52, 53, 54, 61, 63, 66, 68, 69, 72, 76, 77, 78, 82, 88,
        90, 92, 95, 98, 146, 9, 16, 28, 40, 48, 50, 94, 97, 166, 1001, 113, 4, 3, 2, 1
    ]
    vacancies = [
        'Менеджер по продажам',
        'DevOps',
        'Руководитель проектов',
        'Аналитик',
        'Data Engineer',
        'Project manager',
        '1С аналитик',
        '1С программист',
        'Менеджер по работе с клиентами',
        'Mobile',
        'IT Project manager',
        'Специалист технической поддержки',
        'Data Analyst',
        'Веб дизайнер',
        '1C оператор',
        'Backend',
        'QA инженер',
        'Data Scientist',
        '1С администратор',
        'Бизнес аналитик',
        'Product manager',
        '1С методист',
        'Руководитель проектов 1С',
        'Системный аналитик',
        'Технический писатель',
        '1C консультант',
        '1С архитектор',
        'IT Product manager',
        '1С эксперт',
        'Frontend'
    ]
    result = collect_vacancies_data(cities, vacancies)
    print(result['url'])


if __name__ == '__main__':
    main()
