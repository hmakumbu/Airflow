from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG to run every 5 minutes
dag = DAG(
    dag_id='dag_every_5_minutes',
    default_args=default_args,
    description='A DAG that runs every 5 minutes',
    schedule_interval='*/5 * * * *',  # Runing every 5 minutes
)

# Bash task
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Running Bash task every 5 minutes"',
    dag=dag,
)

# Python task
def hello_airflow():
    print("Hello from Airflow, running every 5 minutes")

python_task = PythonOperator(
    task_id='python_task',
    python_callable=hello_airflow,
    dag=dag,
)

# Set task sequence
bash_task >> python_task
