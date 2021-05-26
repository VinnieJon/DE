from datetime import datetime
from airflow import DAG
import requests
import json
import os
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from hdfs import InsecureClient
import logging
from pyspark.sql import SparkSession
import psycopg2
from hooks.base_hook import base_hook
from functions.postgres import *
from functions.robots_out_of_stock import *


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    'daily_etl',
    description='Daily ETL DAG',
    schedule_interval='@daily',
    start_date=datetime(2021,5,26,6),
    default_args=default_args
)


dummy1 = DummyOperator(
    task_id='dummy_1',
    dag=dag
)


dummy2 = DummyOperator(
    task_id='dummy_2',
    dag=dag
)

dummy3 = DummyOperator(
    task_id='dummy_3',
    dag=dag
)


out_of_stock_t1 = PythonOperator(
    task_id='robot_auth',
    python_callable=robot_auth,
    dag=dag
)

out_of_stock_t2 = PythonOperator(
    task_id='robot_out_of_stock',
    python_callable=robot_out_of_stock,
    op_kwargs={'date':'{{ds}}'},
    dag=dag
)

out_of_stock_t3 = PythonOperator(
    task_id='out_of_stock_load_to_silver',
    python_callable=out_of_stock_load_to_silver,
    op_kwargs={'date':'{{ds}}'},
    dag=dag
)

for i in get_tables():
    dummy1 >> operator_psql(i, load_to_bronze_psql, dag, 'bronze') >> dummy2 >> out_of_stock_t1 >> out_of_stock_t2 >> \
    operator_psql(i, data_clear_load_to_silver_psql, dag, 'silver') >> dummy3 >> out_of_stock_t3




