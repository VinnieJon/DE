from datetime import datetime
from airflow import DAG
import os
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from base_download import *
import csv


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'download_db',
    description='Download all tables from DB',
    schedule_interval='@daily',
    start_date=datetime(2021,5,8,14),
    default_args=default_args
)


t1 = PythonOperator(
    task_id='get_postgres_conn',
    python_callable=getconnection,
    dag=dag
)


t2 = PythonOperator(
    task_id='write_aisles',
    python_callable=writerrecords_aisles,
    dag=dag
)

t3 = PythonOperator(
    task_id='write_clients',
    python_callable=writerrecords_clients,
    dag=dag
)

t4 = PythonOperator(
    task_id='write_departments',
    python_callable=writerrecords_departments,
    dag=dag
)

t5 = PythonOperator(
    task_id='write_orders',
    python_callable=writerrecords_orders,
    dag=dag
)

t6 = PythonOperator(
    task_id='write_products',
    python_callable=writerrecords_products,
    dag=dag
)


t1 >> t2 >> t3 >> t4 >> t5 >> t6