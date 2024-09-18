from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2024, 9, 18), schedule_interval="@daily", catchup=False) as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")
    time_date = BashOperator(task_id="date_time", bash_command="date")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> time_date



