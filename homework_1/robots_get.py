import requests
import json
import yaml
from requests import HTTPError

def load_config(config_path):
    with open(config_path, mode='r') as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config['app']


def robot_auth(config):

    auth = config['auth']
    headers = {'content-type': 'application/json'}
    data = {'username': config['username'], 'password': config['password']}

    r = requests.post(auth, data=json.dumps(data), headers=headers, timeout=10)
    token = r.json()['access_token']

    return token

def robot_get(config, token):

    url = config['url']
    headers = {'content-type': 'application/json', 'Authorization': 'JWT ' + token}

    for date in config['dates']:
        data = {'date': date}
        try:
            r = requests.get(url, data=json.dumps(data), headers=headers, timeout=10)
            data = r.json()
        except HTTPError:
            continue

        with open(f'./data/{date}.json', 'w') as json_file:
            json.dump(data, json_file)



if __name__ == '__main__':
    config = load_config('./config.yaml')
    token = robot_auth(config)
    robot_get(config, token)
