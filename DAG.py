import pendulum
import logging
​
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime
from hdfs import InsecureClient
from urllib.request import urlopen
from airflow.decorators import dag
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
​
TASK_LOGGER = logging.getLogger('hackathon')
​
POSTGRES_DE_CONN_ID = PostgresHook.get_connection('postgres')
POSTGRES_HOST = POSTGRES_DE_CONN_ID.host
POSTGRES_PORT = POSTGRES_DE_CONN_ID.port
POSTGRES_USER = POSTGRES_DE_CONN_ID.login
POSTGRES_PASSWORD = POSTGRES_DE_CONN_ID.password
POSTGRES_DB = POSTGRES_DE_CONN_ID.schema
POSTGRES_URL = f'jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
HDFS_CONN = HttpHook.get_connection('hdfs')
NAME_NODE_URL = HDFS_CONN.host
NAME_NODE_USER = HDFS_CONN.login
​
​
def get_file(**kwargs):
    date_yyyy_mm_dd = kwargs['ds']
    date_time = datetime.strptime(date_yyyy_mm_dd, '%Y-%m-%d')
    date_yyyy_bbb_dd = date_time.strftime('%Y-%b-%d')
    url = f'https://data.ijklmn.xyz/events/events-{date_yyyy_bbb_dd}-2134.json.zip'
    hdfs_client = InsecureClient(NAME_NODE_URL, NAME_NODE_USER)
    TASK_LOGGER.info(url)
    try:
        resp = urlopen(url)
        myzip = ZipFile(BytesIO(resp.read()))
        with myzip.open("events-2022-Sep-30-2134.json", 'r') as f:
            content = f.read()
            with hdfs_client.write('/user/ubuntu/raw_data/events/load_date=2022-09-30/src.json', overwrite=True) as json_file:
                json_file.write(content)
                
    except: raise Exception("Error with with url")
​
​
@dag(description='Provide default dag for de hack',
     schedule_interval='@daily',
     start_date=pendulum.parse('2022-10-01'),
     catchup=False,
     tags=['hackathon', 'project']
     )
​
def hak_project():
​
    upload_file = PythonOperator(
        task_id='download_file_to_hdfs',
        python_callable=get_file
    )
​
    events_partitioned = SparkSubmitOperator(
        task_id='events_partitioned',
        application='repartition.py',
        application_args=['2022-09-30', '/user/ubuntu/raw_data/events', '/user/ubuntu/silver/events'],
    )
​
    upd_shopping_list = SparkSubmitOperator(
        task_id='upd_shopping_list',
        application='jobs/shopping_list.py',
        application_args=['marts.ivi_shopping_list', POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, ['/user/ubuntu/silver/events']],
    )
​
    job_events = SparkSubmitOperator(
        task_id='job_events.py',
        application='jobsjoba_events.py',
        application_args=['jobs/user/ubuntu/silver/events', 'marts.events_count'],
    )
​
    job_events = SparkSubmitOperator(
        task_id='job_events.py',
        application='jobs/joba_payments.py',
        application_args=['/user/ubuntu/silver/events', 'marts.events_count'],
    )

​
    upload_file >> events_partitioned >> [upd_shopping_list, job_events, job_payments]
​
_ = hak_project()
