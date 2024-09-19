import requests
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

GITHUB_REPO = "Airflow" 
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_WORKFLOW = "tempflow.yml"
API_KEY = os.getenv('weather_api')
CITY = 'senegal'


def trigger_github_workflow(temperature, **kwargs):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW}/dispatches"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    data = {
        "ref": "main",  
        "inputs": {
            "temperature": str(temperature)
        }
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 204:
        print("GitHub Actions workflow triggered successfully!")
    else:
        print(f"Failed to trigger GitHub Actions workflow: {response.status_code}")
        print(response.text)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='weather_dag_with_github_action',
    default_args=default_args,
    description='Fetch, process, save weather data, and trigger GitHub Actions',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 19),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    def fetch_temperature_from_api(**kwargs):
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}?unitGroup=us&key={API_KEY}&contentType=json"
        response = requests.get(url)
        data = response.json()
        temp_celsius = data['currentConditions']['temp']
        return  temp_celsius

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_temperature_from_api,
        provide_context=True,
    )

    trigger_github = PythonOperator(
        task_id='trigger_github',
        python_callable=trigger_github_workflow,
        op_kwargs={'temperature': '{{ task_instance.xcom_pull(task_ids="fetch_weather") }}'},
        provide_context=True,
    )

    end = DummyOperator(task_id='end')

    start >> fetch_weather >> trigger_github >> end
