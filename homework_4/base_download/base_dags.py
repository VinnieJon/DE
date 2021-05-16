from datetime import datetime
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
import psycopg2
from base_download import *


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'get_base_data',
    description='Download DB',
    schedule_interval='@daily',
    start_date=datetime(2021,5,16,17),
    default_args=default_args
)


t1 = PythonOperator(
    task_id='base_download',
    python_callable=base_download,
    op_kwargs={'date':'{{ds}}'},
    dag=dag
)




