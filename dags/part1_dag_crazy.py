from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from pendulum import datetime

def create_dag(dag_id, schedule, default_args):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def crazy_recursive_dag():
        
        @task
        def task_node(id):
            print("Hello. This is task: {}".format(str(id)))
        
        def recursive_tasks(level):
            if level == 0:
                return task_node(level)
            else:
                parent_task = recursive_tasks(level-1) 
                child_1 = recursive_tasks(level-1) 
                child_2 = recursive_tasks(level-1) 
                parent_task >> child_1
                parent_task >> child_2
                return parent_task
                
        # Don't go over 5 levels on your slow local machine :)
        recursive_tasks(1) 
             
    generated_dag = crazy_recursive_dag()
    return generated_dag


# build the dag
dag_id = "part1_dag_crazy"
default_args = {"owner": "santiago", "start_date": datetime(2023, 8, 27)}
schedule = "@daily"
dag_number = 1

globals()[dag_id] = create_dag(dag_id, schedule, default_args)