import os
import logging
import asyncio
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sqlite3
import numpy as np
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.text import Tokenizer

# Airflow의 기본 로거 사용
logger = logging.getLogger('airflow.task')

class SentimentAnalyzer:
    def __init__(self, model_path):
        # .h5 모델 불러오기
        self.model = load_model(model_path, compile=False)
        self.model.compile(optimizer='adam', loss='sparse_categorical_crossentropy')
        # 사전 학습된 토크나이저 설정
        self.tokenizer = Tokenizer(num_words=None, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n', lower=True)

    def analyze_sentiment(self, text):
        try:
            # 텍스트 전처리 및 예측 수행
            sequence = self.tokenizer.texts_to_sequences([text])
            padded_sequence = pad_sequences(sequence, maxlen=100, padding='post')
            prediction = self.model.predict(padded_sequence)
            sentiment_index = np.argmax(prediction)  # 가장 높은 확률의 인덱스 추출
            prediction_label = 'positive' if sentiment_index == 2 else 'negative' if sentiment_index == 0 else 'neutral'
            logger.info(f"Predicted label: {prediction_label}")
            return prediction_label
        except Exception as e:
            logger.error(f"Sentiment analysis error: {e}")
            return None

async def analyze_and_update_sentiment(analyzer, conn, comment_id, text):
    try:
        logger.info(f"Starting analysis for Comment ID: {comment_id}")
        sentiment_result = await asyncio.get_running_loop().run_in_executor(
            None, analyzer.analyze_sentiment, text
        )
        if sentiment_result is not None:
            cursor = conn.cursor()
            cursor.execute("UPDATE comments SET sentiment = ? WHERE comment_id = ?", (sentiment_result, comment_id))
            conn.commit()
            logger.info(f"Comment ID: {comment_id}, Sentiment: {sentiment_result}")
    except Exception as e:
        logger.error(f"Error updating sentiment for Comment ID {comment_id}: {e}")

async def update_comments_sentiment(analyzer, conn):
    cursor = conn.cursor()
    # query = "SELECT comment_id, text FROM comments WHERE sentiment IS NULL"
    query = """
    SELECT comment_id, text
    FROM (
        SELECT 
            comment_id, 
            text,
            ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY published_at) AS row_num
        FROM comments
        WHERE sentiment IS NULL
    ) AS numbered_comments
    WHERE row_num <= 500
    """
    cursor.execute(query)
    comments_to_analyze = cursor.fetchall()
    logger.info(f"Number of comments to analyze: {len(comments_to_analyze)}")

    semaphore = asyncio.Semaphore(5)  # 동시에 실행할 비동기 작업 수 제한

    async def limited_analyze(comment_id, text):
        async with semaphore:
            await analyze_and_update_sentiment(analyzer, conn, comment_id, text)

    tasks = [limited_analyze(comment_id, text) for comment_id, text in comments_to_analyze]
    await asyncio.gather(*tasks)

async def process_comments_async():
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'db', "comments.db")
    conn = sqlite3.connect(db_path, isolation_level=None)  # 비동기 작업에서 동일한 연결을 공유하도록 설정
    model_path = 'dags/project_test2.h5'
    analyzer = SentimentAnalyzer(model_path)

    await update_comments_sentiment(analyzer, conn)
    conn.close()

def run_process_comments():
    asyncio.run(process_comments_async())
