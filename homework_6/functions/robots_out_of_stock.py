import requests
import json
import os
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from hdfs import InsecureClient
from hooks.base_hook import base_hook
import logging
from pyspark.sql import SparkSession

def robot_auth():
    conn = base_hook('robot_auth')
    auth = conn['host']
    headers = {'content-type': 'application/json'}
    data = {'username': conn['user'], 'password': conn['password']}

    r = requests.post(auth, data=json.dumps(data), headers=headers, timeout=10)
    token = r.json()['access_token']

    Variable.set('robot_token', token)

def robot_out_of_stock(date):
    conn = base_hook('robot_out_of_stock')

    url = conn['host']
    token = Variable.get('robot_token')

    headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
    data = {'date': date}

    r = requests.get(url, data=json.dumps(data), headers=headers, timeout=10)
    r = r.json()

    if r['message'] == "No out_of_stock items for this date":
        print(f'There is no out_of_stock items for {date}')
        logging.info(f'There is no out_of_stock items for {date}')

    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    with client.write(
        os.path.join('/bronze/out_of_stock', date + '_out_of_stock.json'), encoding='utf-8') as f:
        json.dump(r, f)
        logging.info('File loaded ' + os.path.join('/bronze/out_of_stock', date + '_out_of_stock.json'))


def out_of_stock_load_to_silver(date):

    spark = SparkSession.builder \
        .master('local') \
        .appName('out_of_stock') \
        .getOrCreate()

    path = os.path.join('/bronze/out_of_stock', date + '_out_of_stock.json')
    df = spark.read.json(path)

    if 'message' in df.schema.names:
        return
    else:
        df.write.parquet(os.path.join('/silver/out_of_stock', date), mode="append")
        logging.info(os.path.join('/silver/out_of_stock', date) + ' saved to silver')



