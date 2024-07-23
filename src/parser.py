import pandas as pd
import requests
import logging
import time


def collect_vacancies_data(cities, vacancies):
    url = 'https://api.hh.ru/vacancies'
    logging.info(f'Starting vacancy requests using {url=}')
    timestamp_now = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    all_vacancies = []
    for vacancy_index, vacancy in enumerate(vacancies, start=1):
        logging.info(f"Processing {vacancy_index}/{len(vacancies)} specialties: {vacancy}")
        for city_id in cities:
            for page in range(20):
                params = {
                    'text': vacancy,
                    'area': city_id,
                    'per_page': '100',
                    'search_field': 'name',
                    'page': page
                }
                try:
                    response = requests.get(url, params=params)
                    time.sleep(0.5)

                    if response.status_code == 200:
                        vacancies_data = response.json()
                        if not vacancies_data['items']:
                            break

                        df = pd.DataFrame(vacancies_data['items'])
                        df['income_name'] = vacancy
                        df['collected_at'] = timestamp_now
                        all_vacancies.append(df)
                    else:
                        logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
                        logging.error(f"Response content: {response.text}")
                        break

                except Exception as e:
                    logging.error(f"An error occurred: {e}")
                    break

            logging.info(f"Processed city {city_id}")

    logging.info(f"Processed {vacancy_index} out of {len(vacancies)} vacancies")

    if all_vacancies:
        result = pd.concat(all_vacancies, ignore_index=True)
        result.sort_values(by=['id'], inplace=True)
        logging.info(f"Total number of initially collected vacancies: {len(result)}")

        result_uniq = result.drop_duplicates(subset=['id']).reset_index(drop=True)
        logging.info(f"Number of unique vacancies: {len(result_uniq)}")
    else:
        result_uniq = pd.DataFrame()
        logging.info("No vacancies collected.")

    return result_uniq
