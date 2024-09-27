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



# key = AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM


def search_videos_sessions():
    try:
        # YouTube API 정보
        API_KEY = 'AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM'  # API 키 입력
        VIDEO_ID = 'video_list'  # 조회할 비디오의 ID 영상 
        #목록db를 받아서 for문으로 돌려서 조건에 맞게 진행
        #videos.db의 id를 연동해서 id가 들어갈수 있게 진행 

        # YouTube Data API 요청 URL
        url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={VIDEO_ID}&key={API_KEY}'

        # API 호출
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()

            # 데이터 파싱
            video_info = data['items'][0]
            video_id = video_info['id']
            view_count = video_info['statistics']['viewCount']
            like_count = video_info['statistics']['likeCount']
            dislike_count = video_info['statistics'].get('dislikeCount', 0)  # dislikeCount가 없으면 0으로 표기
            comment_count = video_info['statistics']['commentCount']
            collected_at = datetime.now().strftime('%Y-%m-%d')

            # 로그 출력
            logging.info(f"Video ID: {video_id}")
            logging.info(f"조회수: {view_count}")
            logging.info(f"좋아요 수: {like_count}")
            logging.info(f"싫어요 수: {dislike_count}")
            logging.info(f"댓글 수: {comment_count}")
            logging.info(f"세션 수집 날짜: {collected_at}")

            # SQLite DB 연결
            conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db',"videos_sessions.db"))  # DB 파일 생성 또는 연결
            cursor = conn.cursor()

            # 테이블 생성
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos_sessions (
                video_id TEXT,
                view_count INTEGER,
                like_count INTEGER,
                dislike_count INTEGER,
                comment_count INTEGER,
                collected_at TEXT
            )
            ''')

            # 데이터 삽입
            cursor.execute('''
            INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at))

            # 변경 사항 저장 및 연결 종료
            conn.commit()
            conn.close()

            print("데이터베이스에 성공적으로 데이터가 저장되었습니다.")
        else:
            logging.error(f"API 호출 실패: 상태 코드 {response.status_code}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"API 호출 중 오류 발생: {e}")
    except sqlite3.Error as e:
        logging.error(f"SQLite 데이터베이스 오류 발생: {e}")
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")

# 함수 실행
search_videos_sessions()



        #데이터를 BigQuery에 저장 
        #credentials = service_account.Credentials.from_service_account_file(
        #    os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','data-infra-project-428914-95324759a136.json'))
        #client = bigquery.Client(credentials=credentials, project=credentials.project_id)
       

       # BigQuery에 삽입할 데이터 구성
       # table_id = 'data-infra-project-428914.train.video_sessions'  # 테이블 ID
       # rows_to_insert = [
        #    {
        #        'video_id': video_id,
        #        'view_count': view_count,
        #        'like_count': like_count,
        #        #'dislike_count': dislike_count,
        #        'comment_count': comment_count,
        #        'collected_at': collected_at
        #    }
       # ]
    
      # 데이터 삽입
        #errors = client.insert_rows_json(table_id, rows_to_insert)
        #if errors == []:
        #    print("New rows have been added.")
        #if errors:
       #     print(f"Encountered errors while inserting rows: {errors}")
  #  else:
       # logging.error(f"Failed to fetch video data: {response.status_code}, {response.text}")

        #함수안에 있는건 try,except(에러상황)로 감싸주기 
    

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'search_videos_sessions',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='search_videos_sessions',
    python_callable=search_videos_sessions,
    dag=dag
)

#파일명, 기본api함수명, dag부분 task부분, task id 파일명이랑 똑같이 . 

#9/22 데이터 삽입 부분(SQL), dislike예외처리 