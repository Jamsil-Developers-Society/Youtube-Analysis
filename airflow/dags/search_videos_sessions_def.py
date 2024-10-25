from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys

# Airflow에서 사용할 기본 인수
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 생성
with DAG(
    'search_videos_sessions',
    default_args=default_args,
    description='Fetch video data asynchronously and store in DB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    # search_videos_sessions.py의 비동기 래퍼 함수 불러오기
    from search_videos_sessions_func import run_search_videos_sessions
    

    # PythonOperator로 비동기 래퍼 함수 실행
    fetch_video_data = PythonOperator(
        task_id='fetch_video_data_task',
        python_callable=run_search_videos_sessions,
    )

    fetch_video_data
# 이거 템플릿으로 안에 변수들은 우리가 쓰던 파일에서 쓰던거 그대로 가져오고