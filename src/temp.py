# import requests
import json
import time
import logging
import hashlib
from datetime import datetime
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# import psycopg2.extras

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("airflow.task")


def insert_vacancies(income_name, collected_at, vacancies):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import psycopg2.extras
    pg_hook = PostgresHook(postgres_conn_id='hh-db-2024')
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO stg.raw_vacancies (income_name, collected_at, secondary_dict, hash)
            VALUES %s
            ON CONFLICT (hash) DO NOTHING;
            """
            values = []
            for vacancy in vacancies:
                vacancy_json = json.dumps(vacancy)
                vacancy_hash = hashlib.md5(vacancy_json.encode()).hexdigest()
                values.append((income_name, collected_at, vacancy_json, vacancy_hash))

            psycopg2.extras.execute_values(cursor, insert_query, values)
            conn.commit()
    logger.info(f"Inserted {len(vacancies)} vacancies for {income_name} at {collected_at}")


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


def parse_hh_vacancies(areas_list, income_names_list, batch_size=100):
    import requests
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
