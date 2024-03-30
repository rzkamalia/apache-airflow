from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook # allows you to easly interact with an external tool/service

from datetime import datetime, timedelta
import json
from pandas import json_normalize

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

def _process_user(ti):
    '''
        to pull the data that has been downloaded by task extract_user
    '''

    user = ti.xcom_pull(task_ids = 'extract_user')
    user = user['results'][0]
    print(user)
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['password'],
        'password': user['login']['password'],
        'email': user['email']
    })
    processed_user.to_csv('processed_user.csv')

def _store_user():
    '''
        to insert data from csv to postgres
    '''

    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY newest_users FROM stdin WITH DELIMITER as ','",
        filename = '/opt/airflow/processed_user.csv'
    )

with DAG(
    dag_id = 'user_processing',
    default_args = default_args,
    start_date = datetime(2023, 1, 1, 00, 00, 00),
    schedule_interval = '@daily',
    catchup = False
) as dag:
    # to create table
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = 
            ''' 
                CREATE TABLE IF NOT EXISTS newest_users (
                    no TEXT NOT NULL,
                    firstname TEXT NOT NULL,
                    lastname TEXT NOT NULL,
                    country TEXT NOT NULL,
                    username TEXT NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT NOT NULL
                )
            '''
    )

    # to check the API is available or not
    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'user_api',
        endpoint = 'api/'
    )

    # to extract user
    extract_user = SimpleHttpOperator(
        task_id = 'extract_user',
        http_conn_id = 'user_api',
        endpoint = 'api/',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    # to load user
    process_user = PythonOperator(
        task_id = 'process_user',
        python_callable = _process_user
    )

    # to store user
    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable = _store_user
    )

    create_table >> is_api_available >> extract_user >> process_user >> store_user