from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
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
    dag_id='xcom_dag',
    default_args=default_args,
    description='A DAG using XCom for communication',
    schedule_interval=timedelta(days=1),
)

# Define the Python task to push the current timestamp to XCom
def push_timestamp(ti):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ti.xcom_push(key='timestamp', value=timestamp)

push_task = PythonOperator(
    task_id='push_timestamp',
    python_callable=push_timestamp,
    dag=dag,
)

# Define the Bash task to pull the value from XCom and echo it
bash_task = BashOperator(
    task_id='pull_and_echo_timestamp',
    bash_command='echo "Timestamp from XCom: {{ ti.xcom_pull(key="timestamp") }}"',
    dag=dag,
)

# Set task sequence
push_task >> bash_task
