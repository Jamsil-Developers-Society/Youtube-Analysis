import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import pandas as pd
import sqlite3
import logging
# from dotenv import load_dotenv

# load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)),'env','.env'))

def search_categories():
    # key = os.getenv("Google_Token")
    # logging.info("key : "+key)
    try:
        # response = requests.get(f"https://www.googleapis.com/youtube/v3/videoCategories?key={ key }&regionCode=KR").json()
        response = requests.get(f"https://www.googleapis.com/youtube/v3/videoCategories?key=AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM&regionCode=KR").json()
        # print(response)
        logging.info(f"Total categories count : { len(response['items']) }")

        is_exists = os.path.exists(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','categories.db'))

        conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','categories.db'))

        if conn:
            logging.info("DB Connect success")
        else:
            Exception("DB connect fail")
        
        df = pd.DataFrame([(int(item['id']), item['snippet']['title']) for item in response['items']], columns=['category_id', 'category_title'], index=None)
        # df = pd.DataFrame([(46, "asdfg")], columns=['category_id', 'category_title'], index=None)

        logging.info(df)

        if is_exists:
            # DB 파일이 있을 때
            logging.info("DB file Exist")
            # df_origin = pd.read_sql_table('categories', conn)
            df_origin = pd.read_sql(
                """
                SELECT category_id, category_title FROM categories
                """
                ,conn)
            df_filtered = df[~df['category_id'].isin(df_origin['category_id'])]
            df_filtered.to_sql('categories', conn, if_exists='append')
        else:
            # DB 파일이 없을 때
            logging.info("Create DB file & table")
            df.to_sql('categories', conn, if_exists='fail')
    except Exception as e:
        logging.error(e)

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'search_categories',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='search_categories',
    python_callable=search_categories,
    dag=dag
)
