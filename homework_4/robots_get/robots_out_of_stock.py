import requests
import json
import os
from requests import HTTPError
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from hdfs import InsecureClient

def robot_auth():
    conn = BaseHook.get_connection('robot_auth')

    auth = conn.host
    headers = {'content-type': 'application/json'}
    data = {'username': conn.login, 'password': conn.password}

    r = requests.post(auth, data=json.dumps(data), headers=headers, timeout=10)
    token = r.json()['access_token']


    Variable.set('robot_token', token)

def robot_out_of_stock(date):
    conn = BaseHook.get_connection('robot_out_of_stock')

    url = conn.host
    token = Variable.get('robot_token')

    headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}
    data = {'date': date}

    r = requests.get(url, data=json.dumps(data), headers=headers, timeout=10)
    r = r.json()

    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    with client.write(f'/bronze/out_of_stock/{date}.json', encoding='utf-8') as f:
        json.dump(r, f)


    #     if not os.path.exists(os.path.join(os.getcwd(), 'bronze')):
    #     os.makedirs(os.path.join(os.getcwd(), 'bronze'))
    # elif not os.path.exists(os.path.join(os.getcwd(), 'bronze/out_of_stock')):
    #     os.makedirs(os.path.join(os.getcwd(), 'bronze/out_of_stock'))
    # with open(os.path.join(os.getcwd(), f'bronze/out_of_stock/{date}.json'), 'w') as f:
    #     json.dump(r, f)




