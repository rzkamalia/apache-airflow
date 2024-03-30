from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from datetime import datetime

from subdags.subdag_download import subdag_downloads

# group using subdag

with DAG(
    dag_id = 'group_dag', 
    start_date = datetime(2023, 3, 24, 19, 15, 00), 
    schedule_interval = '@daily', 
    catchup = False
) as dag:
    
    args = {
        'start_date': dag.start_date,
        'schedule_interval': dag.schedule_interval,
        'catchup': dag.catchup
    }
    
    downloads = SubDagOperator(
        task_id = 'downloads',
        subdag = subdag_downloads(dag.dag_id, 'downloads', args)
    )

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