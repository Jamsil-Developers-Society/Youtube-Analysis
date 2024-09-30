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

# 1. search_videos_list에서 video_id를 불러오는 함수
def get_video_ids_from_search_videos_list():

    # videos.db에서 video_id 가져오기
    db_folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),'db',"videos.db")
    conn = sqlite3.connect(db_folder_path)
    
    query = "SELECT id FROM videos"  # videos 테이블에서 id (video_id) 가져오기
    video_ids_df = pd.read_sql(query, conn)
    
    conn.close()  # DB 연결 종료
    return video_ids_df['id'].tolist()  # video_id 리스트로 반환

#API_KEY = 'AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM'  # YouTube Data API 키
  #      url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={VIDEO_ID}&key={API_KEY}'

def search_videos_sessions():

    # video_id 리스트 가져오기 (search_videos_list의 결과 사용)
    video_ids = get_video_ids_from_search_videos_list()  # 여기서 video_ids 리스트를 불러옴
    
    API_KEY = 'AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM'  # YouTube Data API 키
    
     # SQLite DB 연결
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "videos_sessions.db"))  # DB 파일 생성 또는 연결
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

    for video_id in video_ids:  # 각 video_id에 대해 반복
        # API 호출 URL 설정
        url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={API_KEY}'

        try:
            response = requests.get(url)
            if response.status_code != 200:
                logging.error(f"API 호출 실패: 상태 코드 {response.status_code}")
                continue  # 다음 video_id로 넘어감

            data = response.json()

            # 데이터 파싱
            video_info = data['items'][0]
            view_count = video_info['statistics']['viewCount']
            like_count = video_info['statistics']['likeCount']
            dislike_count = video_info['statistics'].get('dislikeCount', 0)  # dislikeCount가 없으면 0으로 표기
            comment_count = video_info['statistics']['commentCount']
            collected_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 로그 출력
            logging.info(f"Video ID: {video_id}")
            logging.info(f"조회수: {view_count}")
            logging.info(f"좋아요 수: {like_count}")
            logging.info(f"싫어요 수: {dislike_count}")
            logging.info(f"댓글 수: {comment_count}")
            logging.info(f"세션 수집 날짜: {collected_at}")

            # 영상별 세션 데이터 수 확인
            query = "SELECT COUNT(*) FROM videos_sessions WHERE video_id = ?"
            cursor.execute(query, (video_id,))
            session_count = cursor.fetchone()[0]

            # 조건 1: 영상별로 세션 데이터가 5개 이하
            if session_count < 5:
                # 데이터 삽입
                cursor.execute('''
                INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at))
                logging.info("데이터베이스에 성공적으로 데이터가 저장되었습니다.")
            else:
                # 조건 3: 최근 세션 데이터 2개를 가져옴
                query = "SELECT view_count FROM videos_sessions WHERE video_id = ? ORDER BY collected_at DESC LIMIT 2"
                cursor.execute(query, (video_id,))
                recent_sessions = cursor.fetchall()

                if len(recent_sessions) == 2:  # 최근 세션 데이터가 2개 있는 경우
                    previous_view_count_1 = recent_sessions[0][0]
                    previous_view_count_2 = recent_sessions[1][0]
                    increase = previous_view_count_1 - previous_view_count_2

                    # 현재 조회수의 10% 계산
                    ten_percent_of_current = int(view_count * 0.1)

                    # 조건 3: 상승폭이 현재 조회수의 10% 이하인 경우
                    if increase <= ten_percent_of_current:
                        logging.info(f"Video ID: {video_id}는 수집하지 않음. 상승폭: {increase}, 현재 조회수의 10%: {ten_percent_of_current}")
                    else:
                        # 데이터 삽입
                        cursor.execute('''
                        INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at))
                        logging.info("데이터베이스에 성공적으로 데이터가 저장되었습니다.")
                # else:
                #     # 세션 데이터가 없는 경우 (조건 3에 해당)
                #     cursor.execute('''
                #     INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at)
                #     VALUES (?, ?, ?, ?, ?, ?)
                #     ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at))
                #     logging.info("세션 데이터가 없으므로, 데이터를 수집하였습니다.")

        except requests.exceptions.RequestException as e:
            logging.error(f"API 호출 중 오류 발생: {e}")
        except sqlite3.Error as e:
            logging.error(f"SQLite 데이터베이스 오류 발생: {e}")
        except Exception as e:
            logging.error(f"예상치 못한 오류 발생: {e}")

    # 변경 사항 저장 및 연결 종료
    conn.commit()
    conn.close()

    #ㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡㅡ

    print("데이터베이스에 성공적으로 데이터가 저장되었습니다.")
    

# 함수 실행
#search_videos_sessions()



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
    'start_date': datetime(2024, 9, 30),
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