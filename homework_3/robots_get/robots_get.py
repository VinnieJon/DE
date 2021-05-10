import requests
import json
import yaml
import os
from requests import HTTPError

def robot_get():
    with open(os.path.join(os.getcwd(), 'config/config.yaml'), mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)['app']

    auth = config['auth']
    headers = {'content-type': 'application/json'}
    data = {'username': config['username'], 'password': config['password']}

    r = requests.post(auth, data=json.dumps(data), headers=headers, timeout=10)
    token = r.json()['access_token']

    url = config['url']
    headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}

    for date in config['dates']:
        data = {'date': date}
        try:
            r = requests.get(url, data=json.dumps(data), headers=headers, timeout=10)
            data = r.json()
        except HTTPError:
            continue

        if not os.path.exists(os.path.join(os.getcwd(), 'data')):
            os.makedirs(os.path.join(os.getcwd(), 'data'))
        with open(os.path.join(os.getcwd(), 'data/result.json'), 'w') as f:
            json.dump(data, f)




