from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime

from groups.group_download import download_tasks

# group using TaskGroup

with DAG(
    dag_id = 'group_dag', 
    start_date = datetime(2023, 3, 24, 19, 15, 00), 
    schedule_interval = '@daily', 
    catchup = False
) as dag:
    downloads = download_tasks()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )   
    
    transform_a = BashOperator(
        task_id = 'transform_a',
        bash_command = 'sleep 10'
    )
    
    transform_b = BashOperator(
        task_id = 'transform_b',
        bash_command = 'sleep 10'
    )
    
    transform_c = BashOperator(
        task_id = 'transform_c',
        bash_command = 'sleep 10'
    )
    
    downloads >> check_files >> [transform_a, transform_b, transform_c]