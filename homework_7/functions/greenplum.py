from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from hooks.base_hook import base_hook
from hdfs import InsecureClient
import logging
from hdfs import InsecureClient
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def gold_auth():
    conn = base_hook('gold_auth')

    gp_url = conn['host']
    gp_properties = {"user": conn['user'], "password": conn['password']}

    spark = SparkSession.builder \
        .config('spark.driver.extraClassPath'
                , '/home/user/shared_folder/postgresql-42.2.20.jar') \
        .master('local') \
        .appName('gold_load') \
        .getOrCreate()

    return gp_url, gp_properties, spark


def get_silver_tables():

    tables = ['aisles', 'clients', 'departments', 'orders', 'products', 'stores']

    return tables



def load_to_gold_orders():

    gp_url, gp_properties, spark = gold_auth()


    orders = spark.read.parquet('/silver/postgres/orders')
    products = spark.read.parquet('/silver/postgres/products')

    orders = orders.join(products, orders.product_id == products.product_id, 'left') \
        .select(orders.order_id, orders.product_id, orders.client_id, orders.store_id \
                , orders.quantity, orders.order_date, products.aisle_id, products.department_id)
    logging.info('Orders transformed')

    orders.write.jdbc(gp_url
                      , table='orders'
                      , properties=gp_properties
                      , mode='overwrite')
    logging.info('Orders loaded to the WH')


def load_to_gold_stores():

    gp_url, gp_properties, spark = gold_auth()


    stores = spark.read.parquet('/silver/postgres/stores')
    store_types = spark.read.parquet('/silver/postgres/store_types')
    location_areas = spark.read.parquet('/silver/postgres/location_areas')

    stores = stores.join(store_types, stores.store_type_id == store_types.store_type_id, 'left') \
        .join(location_areas, stores.location_area_id == location_areas.area_id, 'left') \
        .select(stores.store_id, stores.location_area_id, stores.store_type_id, store_types.type, location_areas.area)

    logging.info('Stores transformed')


    stores.write.jdbc(gp_url
                     , table='stores'
                     , properties=gp_properties
                     , mode='overwrite')
    logging.info('Stores loaded to the WH')


def load_to_gold_products():
    gp_url, gp_properties, spark = gold_auth()

    products = spark.read.parquet('/silver/postgres/products')
    products = products.drop('aisle_id', 'department_id')

    logging.info('Products transformed')

    products.write.jdbc(gp_url
                      , table='products'
                      , properties=gp_properties
                      , mode='overwrite')
    logging.info('Products loaded to the WH')


def load_to_gold(table):
    gp_url, gp_properties, spark = gold_auth()

    table = spark.read.parquet('/silver/postgres/' + table)

    table.write.jdbc(gp_url
                  , table=table
                  , properties=gp_properties
                  , mode='overwrite')
    logging.info(f"{table} loaded to the WH")

def load_to_gold_out_of_stock(date):
    gp_url, gp_properties, spark = gold_auth()

    table = spark.read.parquet(os.path.join('/silver/out_of_stock', date))

    table.write.jdbc(gp_url
                     , table=table
                     , properties=gp_properties
                     , mode='append')
    logging.info(os.path.join('/silver/out_of_stock', date) + ' loaded to the WH')


def operator_green(table, dag):
    if table == 'orders':
        return PythonOperator(
        task_id='load_to_gold_orders',
        python_callable=load_to_gold_orders,
        dag=dag
        )
    elif table == 'stores':
        return PythonOperator(
        task_id='load_to_gold_stores',
        python_callable=load_to_gold_stores,
        dag=dag
        )
    elif table == 'products':
        return PythonOperator(
        task_id='load_to_gold_products',
        python_callable=load_to_gold_products,
        dag=dag
        )
    else:
        return PythonOperator(
        task_id='load_to_gold_' + table,
        python_callable=load_to_gold,
        op_kwargs={'table': table},
        dag=dag
        )