import time
import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

task_logger = logging.getLogger("airflow.task")

def speed_run(seconds):
    time.sleep(seconds)
    task_logger.info("It took me {} seconds.".format(seconds))
    

# Set Default args[]
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2022, 11, 12),
}

# Create the DAG
dag = DAG(dag_id='part1_dag_parallel',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

# Task 1
slowest = PythonOperator(
    task_id = 'Slowest', 
    python_callable = speed_run,
    op_args=[10],
    dag = dag)

# Task 2
slow = PythonOperator(
    task_id = 'Slow', 
    python_callable = speed_run,
    op_args=[5],
    dag = dag)

# Task 3
fast = PythonOperator(
    task_id = 'Fast', 
    python_callable = speed_run,
    op_args=[3],
    dag = dag)

# Task 4
fastest = PythonOperator(
    task_id = 'Fastest', 
    python_callable = speed_run,
    op_args=[1],
    dag = dag)

# Dependencies
# NONE