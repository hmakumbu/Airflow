from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import requests
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Constants
API_KEY = os.getenv('weather_api')
CITY = 'senegal'
TEMP_UNIT = 'Fahrenheit'  # We can use also 'Kelvin' 
OUTPUT_FILE = os.getenv('OUTPUT_FILE')
# OUTPUT_FILE = '/path/to/weather_data.json'

# Fetch weather data from Visual Crossing API
def fetch_weather_data(**kwargs):
    
    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{CITY}?unitGroup=us&key={API_KEY}&contentType=json"
    response = requests.get(url)
    data = response.json()
    temp_celsius = data['currentConditions']['temp']
    kwargs['ti'].xcom_push(key='temp_celsius', value=temp_celsius)
  
    return response

# Convert temperature to desired unit
def convert_temperature(**kwargs):
    ti = kwargs['ti']
    temp_celsius = ti.xcom_pull(task_ids='fetch_weather', key='temp_celsius')
    
    if TEMP_UNIT == 'Fahrenheit':
        temp_converted = (temp_celsius * 9/5) + 32
    elif TEMP_UNIT == 'Kelvin':
        temp_converted = temp_celsius + 273.15
    else:
        raise ValueError(f"Unsupported temperature unit: {TEMP_UNIT}")
    
    ti.xcom_push(key='temp_converted', value=temp_converted)

# Save temperature data to a file
def save_to_file(**kwargs):
    ti = kwargs['ti']
    temp_converted = ti.xcom_pull(task_ids='convert_temperature', key='temp_converted')
    
    with open(OUTPUT_FILE, 'w') as file:
        json.dump({"temperature": temp_converted, "unit": TEMP_UNIT}, file)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_dag',
    default_args=default_args,
    description='A simple DAG to fetch, process, and save weather data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 9, 18),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    fetch_weather = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data,
        provide_context=True,
    )

    convert_temperature = PythonOperator(
        task_id='convert_temperature',
        python_callable=convert_temperature,
        provide_context=True,
    )

    save_file = PythonOperator(
        task_id='save_file',
        python_callable=save_to_file,
        provide_context=True,
    )

    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> fetch_weather >> convert_temperature >> save_file >> end

# if __name__=="__main__":
#     test = fetch_weather_data()
#     print(test)
    

