from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='bash_and_python_dag',
    default_args=default_args,
    description='A DAG with Bash and Python tasks',
    schedule_interval=timedelta(days=1),
)

# Define the Bash task
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Running Bash task"',
    dag=dag,
)

# Define the Python task
def hello_airflow():
    print("Hello from Airflow")

python_task = PythonOperator(
    task_id='python_task',
    python_callable=hello_airflow,
    dag=dag,
)

# Set task sequence
bash_task >> python_task
