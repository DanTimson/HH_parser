import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils import setup_session


def fetch_vacancies(session, city_id, vacancy, url, timestamp_now):
    temp_data = []
    for page in range(20):
        params = {
            'text': vacancy,
            'area': city_id,
            'per_page': '100',
            'search_field': 'name',
            'page': page
        }
        try:
            response = session.get(url, params=params)

            if response.status_code == 200:
                vacancies_data = response.json()
                if not vacancies_data['items']:
                    break

                df = pd.DataFrame(vacancies_data['items'])
                df['income_name'] = vacancy
                df['collected_at'] = timestamp_now
                temp_data.append(df)
            else:
                logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
                logging.error(f"Response content: {response.text}")
                break

        except Exception as e:
            logging.error(f"An error occurred: {e}",
                          exc_info=True)
            break
    return temp_data


def process_vacancy_requests(session, cities, vacancies, url, timestamp_now):
    all_vacancies = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_vacancy = {
            executor.submit(fetch_vacancies, session, city_id, vacancy, url, timestamp_now): (city_id, vacancy)
            for vacancy in vacancies for city_id in cities
        }

        for future in as_completed(future_to_vacancy):
            city_id, vacancy = future_to_vacancy[future]
            try:
                temp_data = future.result()
                all_vacancies.extend(temp_data)
                logging.info(f"Processed city {city_id} for vacancy {vacancy}")
            except Exception as e:
                logging.error(f"An error occurred while processing city {city_id} for vacancy {vacancy}: {e}",
                              exc_info=True)

    return all_vacancies


def collect_vacancies_data(cities, vacancies):
    url = 'https://api.hh.ru/vacancies'
    logging.info(f'Starting vacancy requests using {url=}')
    timestamp_now = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    session = setup_session()
    all_vacancies = process_vacancy_requests(session, cities, vacancies, url, timestamp_now)

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
