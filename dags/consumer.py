from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime   

my_file_1 = Dataset('/opt/airflow/my_file_1.txt')
my_file_2 = Dataset('/opt/airflow/my_file_2.txt')
my_file_3 = Dataset('/opt/airflow/my_file_3.txt')
my_file_4 = Dataset('/opt/airflow/my_file_4.txt')
my_file_5 = Dataset('/opt/airflow/my_file_5.txt')

with DAG(
    dag_id = "consumer",
    schedule = [my_file_1, my_file_2, my_file_3, my_file_4, my_file_5],
    start_date = datetime(2023, 3, 24, 19, 15, 00),
    catchup = False
):
    @task()
    def read_dataset():
        with open(my_file_1.uri, 'r') as f:
            print(f.read())

        with open(my_file_2.uri, 'r') as f:
            print(f.read())

        with open(my_file_3.uri, 'r') as f:
            print(f.read())

        with open(my_file_4.uri, 'r') as f:
            print(f.read())

        with open(my_file_5.uri, 'r') as f:
            print(f.read())

    read_dataset()