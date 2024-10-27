import logging
import os
import sqlite3
import pandas as pd # type: ignore
from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta

#logger setting
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

def sentiment_comments():

    try:
        logger.info('DB_connect') #DB_connect
        conn_comments = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "comments.db"))
        conn_sessions = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "videos_sessions.db"))
        query_comments = "SELECT * FROM comments;"
        query_sessions = "SELECT video_id, view_count, like_count FROM videos_sessions;"
    
        logger.info('read to DataFrame') #read to DataFrame
        comments_data = pd.read_sql_query(query_comments, conn_comments)
        sessions_data = pd.read_sql_query(query_sessions, conn_sessions)
        combined_data = comments_data.merge(sessions_data, on = 'video_id', how = 'left')

        logger.info('data_set') #data_set
        X = combined_data.drop(columns = ['target_column']).values #Feature data
        y = combined_data['target_column'].values                  #Label data

        # 감성 분석 모델로 sentiment 컬럼 채우는 로직 추가 필요

    except sqlite3.Error as e:
        logger.error(f"ERROR: {e}")

    except Exception as e:
        logger.error(f"ERROR: {e}")

    finally:
        if conn_comments: conn_comments.close()
        if conn_sessions: conn_sessions.close()
        if combined_data:
            logger.info(combined_data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sentiment_comments',
    default_args = default_args,
    description = 'DB > Model to airflow',
    schedule_interval = '@daily'
)

machineLearning_comment_task = PythonOperator(
    task_id = 'sentiment_comments',
    python_callable = sentiment_comments,
    dag = dag
)