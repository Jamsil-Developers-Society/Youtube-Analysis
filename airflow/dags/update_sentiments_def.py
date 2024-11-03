import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
#from dotenv import load_dotenv
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import logging
import asyncio
from transformers import pipeline
# update_sentiments_def 파일에서 비동기 감성 분석 함수 가져오기
from update_sentiments_func import run_process_comments


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'update_sentiments',
    default_args=default_args,
    description='Process sentiment data asynchronously',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 3),
    catchup=False,
) as dag:

    process_comments_task = PythonOperator(
        task_id='process_comments_task',
        python_callable=run_process_comments,
    )

    

process_comments_task 