from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
import json

from transform_data import transform_data
from load_data import load_data

default_args = {
    'owner': 'dyath',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 30),
    'email': 'my_email@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_dag', default_args = default_args, schedule_interval = '@daily', catchup=False) as dag:
    
    is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Romainville&appid=509c897a2f4701c9aba5822533a1a0a6'
        )
    
    extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Romainville&appid=509c897a2f4701c9aba5822533a1a0a6',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )
    
    transform_weather_data = PythonOperator(
        task_id= 'transform_weather_data',
        python_callable=transform_load_data
        )
    
    load_weather_data = PythonOperator(
        task_id= 'load_weather_data',
        python_callable=load_data
        )
    
    is_weather_api_ready >> extract_weather_data >> transform_weather_data >> load_weather_data