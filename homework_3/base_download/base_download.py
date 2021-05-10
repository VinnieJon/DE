import os
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv

PostgresConn = PostgresHook(postgres_conn_id='postgresql_conn')

def getconnection():
    PostgresConn.get_conn()
    print("connected")

def writerrecords_aisles():
    id = PostgresConn.get_records(sql='SELECT * FROM aisles')
    print(id)
    if not os.path.exists(os.path.join(os.getcwd(), 'base_data')):
        os.makedirs(os.path.join(os.getcwd(), 'base_data'))

    with open(os.path.join(os.getcwd(), 'base_data/aisles.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(id)

def writerrecords_clients():
    id = PostgresConn.get_records(sql='SELECT * FROM clients')

    if not os.path.exists(os.path.join(os.getcwd(), 'base_data')):
        os.makedirs(os.path.join(os.getcwd(), 'base_data'))

    with open(os.path.join(os.getcwd(), 'base_data/clients.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(id)

def writerrecords_departments():
    id = PostgresConn.get_records(sql='SELECT * FROM departments')

    if not os.path.exists(os.path.join(os.getcwd(), 'base_data')):
        os.makedirs(os.path.join(os.getcwd(), 'base_data'))

    with open(os.path.join(os.getcwd(), 'base_data/departments.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(id)

def writerrecords_orders():
    id = PostgresConn.get_records(sql='SELECT * FROM orders')

    if not os.path.exists(os.path.join(os.getcwd(), 'base_data')):
        os.makedirs(os.path.join(os.getcwd(), 'base_data'))

    with open(os.path.join(os.getcwd(), 'base_data/orders.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(id)

def writerrecords_products():
    id = PostgresConn.get_records(sql='SELECT * FROM products')

    if not os.path.exists(os.path.join(os.getcwd(), 'base_data')):
        os.makedirs(os.path.join(os.getcwd(), 'base_data'))

    with open(os.path.join(os.getcwd(), 'base_data/products.csv'), 'w') as f:
        writer = csv.writer(f)
        writer.writerows(id)



getconnection()
writerrecords_aisles()
