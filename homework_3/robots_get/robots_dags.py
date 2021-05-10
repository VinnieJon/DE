from datetime import datetime
from airflow import DAG
import requests
import json
import yaml
import os
from requests import HTTPError
from airflow.operators.python_operator import PythonOperator
from robots_get import *


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'get_robot_dreams_data',
    description='Download data from RD API',
    schedule_interval='@daily',
    start_date=datetime(2021,5,8,14),
    default_args=default_args
)


t1 = PythonOperator(
    task_id='robot_get',
    python_callable=robot_get,
    dag=dag
)




