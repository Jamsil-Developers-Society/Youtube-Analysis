from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request as GoogleRequest
import asyncio
import os
import pickle
import aiohttp

# OAuth 2.0 설정
CLIENT_SECRETS_FILE = "/path/to/your/client_secrets.json"
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
API_SERVICE_NAME = "youtube"
API_VERSION = "v3"

# 비동기 자막 다운로드 함수
async def download_caption_async(session, caption_id):
    youtube = get_youtube_service()

    async with session.get(f"https://youtube.googleapis.com/youtube/v3/captions/{caption_id}?key=YOUR_API_KEY") as response:
        if response.status == 200:
            return await response.text()
        else:
            return f"Failed to download caption. Status code: {response.status}"

# OAuth 2.0 자격 증명 가져오기
def get_youtube_service():
    credentials = None

    # 저장된 자격 증명 가져오기
    if os.path.exists("/path/to/token.pickle"):
        with open("/path/to/token.pickle", "rb") as token:
            credentials = pickle.load(token)

    # 자격 증명이 만료되었으면 새로고침
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(GoogleRequest())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_local_server(port=8080)

        with open("/path/to/token.pickle", "wb") as token:
            pickle.dump(credentials, token)

    # YouTube API 클라이언트 생성
    youtube = build(API_SERVICE_NAME, API_VERSION, credentials=credentials)
    return youtube

# 자막 다운로드 작업
def download_caption(video_id):
    youtube = get_youtube_service()

    # 비디오 자막 리스트 가져오기
    captions = youtube.captions().list(part="snippet", videoId=video_id).execute()

    if not captions["items"]:
        print("No captions available for this video.")
        return

    caption_id = captions["items"][0]["id"]  # 첫 번째 자막 ID 가져오기

    # 비동기 처리 설정
    loop = asyncio.get_event_loop()

    async def fetch_captions():
        async with aiohttp.ClientSession() as session:
            return await download_caption_async(session, caption_id)

    caption_data = loop.run_until_complete(fetch_captions())

    # 자막 데이터 처리
    print("Downloaded Caption: ", caption_data)

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('download_caption',
         default_args=default_args,
         description='Download YouTube captions asynchronously using Airflow',
         schedule_interval=timedelta(days=1),
         catchup=False) as dag:

    download_task = PythonOperator(
        task_id='download_caption',
        python_callable=download_caption,
        op_kwargs={'video_id': 'YOUR_VIDEO_ID'},  # 여기에 비디오 ID를 넣으세요.
    )

    download_task
