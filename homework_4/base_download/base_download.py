from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from hdfs import InsecureClient
import psycopg2


def base_download(date):
    conn = BaseHook.get_connection('base_auth')

    pg_creds = {
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()

        cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")

        tables = cursor.fetchall()

        for table in tables:
            with client.write(f'/bronze/base/{date}/{table[0]}.csv') as csv:
                cursor.copy_expert(f'COPY {table[0]} to STDOUT WITH HEADER CSV', csv)






