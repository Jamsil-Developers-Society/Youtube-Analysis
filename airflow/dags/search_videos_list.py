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

def search_videos_list():
    logger = logging.getLogger(' ')
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    #env_folder_path = os.path.join(os.path.dirname(__file__), '..', 'env')
    #env_path = os.path.join(env_folder_path, '.env')
    #if load_dotenv(dotenv_path=env_path):
    #    logger.info(f".env 파일이 성공적으로 로드되었습니다: {env_path}")
    #else:
    #    logging.error(".env 파일을 찾을 수 없거나 로드에 실패했습니다.")
    #api_key = os.getenv("Google_Token")

    db_folder_path = os.path.join(os.path.dirname(__file__), '..', 'db')  # db 폴더의 경로
    db_path = os.path.join(db_folder_path, 'serach_videos_list.db')  # db 폴더 안의 serach_videos_list.db 경로
    conn = sqlite3.connect(db_path)
    rows = pd.DataFrame(columns=['id', 'categoryId', 'publishedAt', 'channelId', 'title', 'description'])

    # 테이블이 이미 존재하는 경우 기존 데이터를 불러옴
    query = "SELECT id FROM search_videos_list"
    try:
        existing_videos = pd.read_sql(query, conn)
    except Exception:
        # 테이블이 아직 없는 경우 빈 DataFrame으로 설정
        existing_videos = pd.DataFrame(columns=['id'])

    #{Google_Token}

    try:
        api_url = 'https://www.googleapis.com/youtube/v3/videos?key=AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM&chart=mostPopular&part=snippet,statistics'
        response = requests.get(api_url)
        if response.status_code != 200:
            logger.error(f"Failed to fetch data from API. Status code: {response.status_code}")
            return

        video_list = response.json()

        for i in range(len(video_list)):
            video_id = video_list['items'][i]['id']

            # 이미 존재하는 비디오인지 확인
            if video_id in existing_videos['id'].values:
                logger.info(f"이미 존재하는 비디오 ID: {video_id}, 추가하지 않음.")
                continue  # 이미 존재하는 비디오는 추가하지 않음

            category_id = video_list['items'][i]['snippet'].get('categoryId', None)
            published_at = video_list['items'][i]['snippet']['publishedAt']
            channel_id = video_list['items'][i]['snippet']['channelId']
            title = video_list['items'][i]['snippet']['title']
            description = video_list['items'][i]['snippet']['description']

            new_row = pd.DataFrame({
                "id": [video_id],
                "categoryId": [int(category_id) if category_id else None],
                "publishedAt": [published_at],
                "channelId": [channel_id],
                "title": [title],
                "description": [description], 
            })
            rows = pd.concat([rows, new_row], ignore_index=True)

        logging.info(f"가져온 데이터: \n{rows}")

        if not rows.empty:
            rows.to_sql('search_videos_list', conn, if_exists='append', index=False)
            logger.info(f"{len(rows)}개의 비디오 데이터를 추가했습니다. ")
        else:
            logger.info("새로 추가할 비디오 데이터가 없습니다.")
        
    except Exception as e:
        logger.error(e);
    
    finally:
        conn.close()


    

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
    'search_videos_list',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='search_videos_list',
    python_callable=search_videos_list,
    dag=dag
)
