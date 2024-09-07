import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import logging

def insert_data_from_api():
    # 로거 설정
    # logger = logging.getLogger(__name__)
    # try:
    #     from google.cloud import bigquery
    #     from google.oauth2 import service_account
    #     print(os.path.dirname(os.path.dirname(__file__))+'/data-infra-project-428914-95324759a136.json')
    #     credentials = service_account.Credentials.from_service_account_file(os.path.dirname(os.path.dirname(__file__))+'/data-infra-project-428914-95324759a136.json')
    #     client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    #     def query_to_bigquery(query):
    #         query_job = client.query(query)
    #         results = query_job.result()
    #         return results.to_dataframe()
    #     query = """
    #     SELECT *
    #     FROM `data-infra-project-428914.train.healthcare_dataset`
    #     LIMIT 1000
    #     """
    #     df = query_to_bigquery(query)
    # except Exception as e:
    #     print(e)
    # pass
    from google.cloud import bigquery
    from google.oauth2 import service_account
    print('asdfg')
    print(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','data-infra-project-428914-95324759a136.json'))
    # credentials = service_account.Credentials.from_service_account_file('/opt/airflow/dags/data-infra-project-428914-95324759a136.json')
    credentials = service_account.Credentials.from_service_account_file(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','data-infra-project-428914-95324759a136.json'))
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    def query_to_bigquery(query):
        query_job = client.query(query)
        results = query_job.result()
        return results.to_dataframe()
    query = """
    SELECT *
    FROM `data-infra-project-428914.train.healthcare_dataset`
    LIMIT 1000
    """
    df = query_to_bigquery(query)

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'search_videos_list',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='search_videos_list',
    python_callable=insert_data_from_api,
    dag=dag
)
