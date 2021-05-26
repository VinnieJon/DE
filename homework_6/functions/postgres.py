from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from hooks.base_hook import base_hook
from hdfs import InsecureClient
import psycopg2
import logging
import os
from pyspark.sql import SparkSession


def get_tables():
    pg_creds = base_hook('base_auth')

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")

        tables = cursor.fetchall()

        logging.info(f'Got all tables')

        return tables


def load_to_bronze_psql(date, table):

    pg_creds = base_hook('base_auth')


    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with client.write(
                os.path.join('/bronze/postgres', date, table[0] + '.csv')
        ) as csv:
            cursor.copy_expert(f'COPY {table[0]} to STDOUT WITH HEADER CSV', csv)

    logging.info('File loaded ' + os.path.join('/bronze/postgres', date, table[0] + '.csv'))


def operator_psql(table, callable, dag, level):
    return PythonOperator(
        task_id='load'+'_'+table[0]+'_'+level,
        python_callable=callable,
        op_kwargs={'table': table, 'date': '{{ds}}'},
        dag=dag
    )

def data_clear_load_to_silver_psql(date, table):
    spark = SparkSession.builder \
        .master('local') \
        .appName('data_clear') \
        .getOrCreate()
    path = os.path.join('/bronze/postgres', date, table[0] + '.csv')
    df = spark.read.csv(path)
    df = df.dropDuplicates()

    df.write.parquet(os.path.join('/silver/postgres', table[0]), mode="overwrite")
    logging.info(f"{table[0]} saved to silver")


