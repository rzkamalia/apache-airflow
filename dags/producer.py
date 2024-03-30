from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime   

my_file_1 = Dataset('/opt/airflow/my_file_1.txt')
my_file_2 = Dataset('/opt/airflow/my_file_2.txt')
my_file_3 = Dataset('/opt/airflow/my_file_3.txt')
my_file_4 = Dataset('/opt/airflow/my_file_4.txt')
my_file_5 = Dataset('/opt/airflow/my_file_5.txt')

with DAG(
    dag_id = "producer",
    schedule = '@daily',
    start_date = datetime(2023, 3, 24, 19, 15, 00),
    catchup = False
):
    @task(outlets = [my_file_1])
    def update_dataset_1():
        with open(my_file_1.uri, 'a+') as f:
            f.write('producer update 1')

    @task(outlets = [my_file_2])
    def update_dataset_2():
        with open(my_file_2.uri, 'a+') as f:
            f.write('producer update 2')

    @task(outlets = [my_file_3])
    def update_dataset_3():
        with open(my_file_3.uri, 'a+') as f:
            f.write('producer update 3')

    @task(outlets = [my_file_4])
    def update_dataset_4():
        with open(my_file_4.uri, 'a+') as f:
            f.write('producer update 4')

    @task(outlets = [my_file_5])
    def update_dataset_5():
        with open(my_file_5.uri, 'a+') as f:
            f.write('producer update 5')
    
    update_dataset_1() >> update_dataset_2() >> update_dataset_3() >> update_dataset_4() >> update_dataset_5()