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



def analyze_sentiment(text):

    distilled_student_sentiment_classifier = pipeline(
    model="lxyuan/distilbert-base-multilingual-cased-sentiments-student", 
    return_all_scores=True
    )
    # 모델을 사용하여 감성 분석 수행
    result = distilled_student_sentiment_classifier(text)
    # 가장 높은 점수의 레이블을 추출
    sentiment = max(result[0], key=lambda x: x['score'])['label']
    return sentiment

# 비동기 댓글 분석 및 업데이트 함수
async def analyze_and_update_sentiment(conn, comment_id, text):
    try:
        logging.info(f"Starting analysis for Comment ID: {comment_id}")
        # 감성 분석 수행
        sentiment_result = await asyncio.get_running_loop().run_in_executor(
            None, analyze_sentiment, text
        )   
        # 분석 결과를 DB에 업데이트
        cursor = conn.cursor()
        cursor.execute("UPDATE comments SET sentiment = ? WHERE comment_id = ?", (sentiment_result, comment_id))
        conn.commit()
        logging.info(f"Comment ID: {comment_id}, Sentiment: {sentiment_result}")

    except Exception as e:
        logging.error(f"오류 발생 (댓글 ID {comment_id}): {e}")

# 비동기 댓글 감성 분석 및 업데이트 처리 함수
async def update_comments_sentiment(conn):
    cursor = conn.cursor()
    
    # sentiment가 None인 댓글 불러오기
    query = "SELECT comment_id, text FROM comments WHERE sentiment IS NULL"
    cursor.execute(query)
    comments_to_analyze = cursor.fetchall()
    logging.info(f"Number of comments to analyze: {len(comments_to_analyze)}")

    semaphore = asyncio.Semaphore(5)  # 동시에 실행할 비동기 작업 수를 5개로 제한

    async def limited_analyze(comment_id, text):
        async with semaphore:  # 동시 작업 제한
            await analyze_and_update_sentiment(conn, comment_id, text)
    
    # 각 댓글에 대해 비동기 분석 및 DB 업데이트
    tasks = [limited_analyze(comment_id, text) for comment_id, text in comments_to_analyze]

    # 비동기 작업 실행
    await asyncio.gather(*tasks)


# 메인 비동기 감성 분석 함수
async def process_comments_async():
    # 데이터베이스 연결
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "comments.db")
    conn = sqlite3.connect(db_path)
    # 비동기 감성 분석 및 DB 업데이트 실행
    await update_comments_sentiment(conn)
    # 연결 종료
    conn.close()


# 비동기 감성 분석 메인 함수 실행
def run_process_comments():
    asyncio.run(process_comments_async())
