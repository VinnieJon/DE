from airflow.hooks.base_hook import BaseHook
import logging


def base_hook(connection):
    conn = BaseHook.get_connection(connection)

    creds = {
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    logging.info(f'Got {connection} creds')
    return creds





