import os
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3
from transformers import pipeline

# 로깅 설정
logger = logging.getLogger(' ')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# 감성 분석 모델 초기화
sentiment_classifier = pipeline(
    model="lxyuan/distilbert-base-multilingual-cased-sentiments-student",
    return_all_scores=True
)

def analyze_sentiment(text):
    try:
        result = sentiment_classifier(text)
        sentiment = max(result[0], key=lambda x: x['score'])['label']
        return sentiment
    except Exception as e:
        logging.error(f"Sentiment analysis error: {e}")
        return None

def update_comments_sentiment():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'db', 'comments.db')
    
    try:
        # 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 분석이 필요한 댓글 불러오기
        cursor.execute("SELECT comment_id, text FROM comments WHERE sentiment IS NULL")
        comments_to_analyze = cursor.fetchall()
        logging.info(f"Number of comments to analyze: {len(comments_to_analyze)}")
        
        # 각 댓글에 대해 감성 분석 및 DB 업데이트
        for comment_id, text in comments_to_analyze:
            sentiment = analyze_sentiment(text)
            if sentiment:
                cursor.execute("UPDATE comments SET sentiment = ? WHERE comment_id = ?", (sentiment, comment_id))
            logging.info(f"Comment ID: {comment_id}, Sentiment: {sentiment}")
        
        conn.commit()
        
    except Exception as e:
        logging.error(f"Error updating comments sentiment: {e}")
        
    finally:
        conn.close()
        logging.info("Database connection closed.")

# Airflow DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2024, 11, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_sentiments_sync',
    default_args=default_args,
    description='Process sentiment data',
    schedule_interval=timedelta(days=1),
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='process_comments_task',
    python_callable=update_comments_sentiment,
    dag=dag
)
