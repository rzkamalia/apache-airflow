from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes = 2)
}
# 'retries': specifies the number of retries that should be attempted in case of task failure.
# 'retry_delay': specifies the delay between retries. it's set to timedelta(minutes = 2), meaning there will be a 2-minute delay between each retry attempt.

with DAG(
    dag_id = 'our_first_dag_v3',
    default_args = default_args,
    description = 'this is our first dag that we write',
    start_date = datetime(2023, 3, 24, 19, 15, 00),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'echo hello world'
    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command = 'echo hello, this is our second task and will be running after first task'
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command = 'echo hello, this is our third task and will be running after first task'
    )

    # # first method
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # # second method
    # task1 >> task2
    # task1 >> task3

    # third method
    task1 >> [task2, task3]