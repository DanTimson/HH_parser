import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils import setup_session

def insert_vacancies(income_name, collected_at, vacancies):
    # from airflow.providers.postgres.hooks.postgres import PostgresHook
#     import psycopg2.extras
#     pg_hook = PostgresHook(postgres_conn_id='hh-db-2024')
#     with pg_hook.get_conn() as conn:
#         with conn.cursor() as cursor:
#             insert_query = """
#             INSERT INTO stg.raw_vacancies (income_name, collected_at, secondary_dict, hash)
#             VALUES %s
#             ON CONFLICT (hash) DO NOTHING;
#             """
#             values = []
#             for vacancy in vacancies:
#                 vacancy_json = json.dumps(vacancy)
#                 vacancy_hash = hashlib.md5(vacancy_json.encode()).hexdigest()
#                 values.append((income_name, collected_at, vacancy_json, vacancy_hash))

#             psycopg2.extras.execute_values(cursor, insert_query, values)
#             conn.commit()
    logger.info(f"Inserted {len(vacancies)} vacancies for {income_name} at {collected_at}")
    

def fetch_vacancies(session, area_id, vacancy, url, timestamp_now):    
    processed_vacancies = {}
    for page in range(20):
        params = {
            'text': vacancy,
            'area': area_id,
            'per_page': '100',
            'search_field': 'name',
            'page': page
        }
        try:
            response = session.get(url, params=params)

            if response.status_code == 200:
                vacancies_data = response.json()
                response.raise_for_status()
                vacancies_list = response.json().get('items', [])
                if not vacancies_list:
                    break

                all_vacancies = []
                for item in vacancies_list:
                    vacancy_data = get_vacancy_details(item, processed_vacancies)
                    if vacancy_data:
                        all_vacancies.append(vacancy_data)

                if all_vacancies:
                    insert_vacancies(vacancy, timestamp_now, all_vacancies)
            else:
                logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
                logging.error(f"Response content: {response.text}")
                break

        except Exception as e:
            logging.error(f"An error occurred: {e}",
                          exc_info=True)
            break
    logger.info(f"Vacancies for city ID {area_id} and job '{vacancy}' successfully collected.")
    
    return temp_data


def parse_hh_vacancies(areas_list, income_names_list, batch_size=100):
    timestamp_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    processed_vacancies = {}

    for income_name in income_names_list:
        for area_id in areas_list:
            page = 0
            while page * batch_size < 2000:
                try:
                    url = 'https://api.hh.ru/vacancies'
                    params = {
                        'text': income_name,
                        'area': area_id,
                        'per_page': batch_size,
                        'search_field': 'name',
                        'page': page
                    }
                    response = requests.get(url, params=params)
                    response.raise_for_status()
                    vacancies_list = response.json().get('items', [])
                    if not vacancies_list:
                        break

                    all_vacancies = []
                    for item in vacancies_list:
                        vacancy_data = get_vacancy_details(item, processed_vacancies)
                        if vacancy_data:
                            all_vacancies.append(vacancy_data)

                    if all_vacancies:
                        insert_vacancies(income_name, timestamp_now, all_vacancies)

                    page += 1
                    time.sleep(1)  # задержка между страницами
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error fetching vacancies list: {e}")
                    break

            logger.info(f"Vacancies for city ID {area_id} and job '{income_name}' successfully collected.")



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


def get_vacancy_details(item, processed_vacancies, retries=3):
    import requests
    vacancy_id = item['id']
    vacancy_url = item['url']
    if vacancy_id not in processed_vacancies:
        processed_vacancies[vacancy_id] = vacancy_url
        vacancy_response = None
        for attempt in range(retries):
            try:
                time.sleep(1)  # задержка между запросами
                vacancy_response = requests.get(vacancy_url)
                vacancy_response.raise_for_status()
                return vacancy_response.json()
            except (requests.exceptions.SSLError, requests.exceptions.HTTPError) as e:
                if isinstance(e, requests.exceptions.HTTPError) and vacancy_response and vacancy_response.status_code != 403:
                    logger.error(f"Error fetching vacancy {vacancy_id}: {e}")
                    return None
                logger.error(f"Error fetching vacancy {vacancy_id}: {e}, retrying...")
                time.sleep(5)  # задержка перед повторной попыткой
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching vacancy {vacancy_id}: {e}")
                return None
        logger.error(f"Failed to fetch vacancy {vacancy_id} after {retries} retries")
        return None
    else:
        existing_url = processed_vacancies[vacancy_id]
        logger.info(f"Vacancy ID {vacancy_id} already processed. Current URL: {vacancy_url}, Existing URL: {existing_url}")
        return None

