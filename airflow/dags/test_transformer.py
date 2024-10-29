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

def test_transformer():
    logger = logging.getLogger(' ')
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    distilled_student_sentiment_classifier = pipeline(
        model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
        return_all_scores=True
    )

    result = distilled_student_sentiment_classifier ("I love this movie and i would watch it again and again!")

    logging.info(result)

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'test_transformer',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='test_transformer',
    python_callable=test_transformer,
    dag=dag
)
