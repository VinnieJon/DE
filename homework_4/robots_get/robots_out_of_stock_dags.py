from datetime import datetime
from airflow import DAG
import requests
import json
import os
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from robots_out_of_stock import *


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'get_robot_dreams_out_of_stock_data',
    description='Download out of stock data from RD API',
    schedule_interval='@daily',
    start_date=datetime(2021,5,16,17),
    default_args=default_args
)


t1 = PythonOperator(
    task_id='robot_auth',
    python_callable=robot_auth,
    dag=dag
)

t2 = PythonOperator(
    task_id='robot_out_of_stock',
    python_callable=robot_out_of_stock,
    op_kwargs={'date':'{{ds}}'},
    dag=dag
)

t1 >> t2




