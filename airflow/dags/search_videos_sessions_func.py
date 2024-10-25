import os
import json
import asyncio
import aiohttp  # 비동기 HTTP 요청을 위한 라이브러리
import pandas as pd
import sqlite3
import logging
from datetime import datetime
from aiohttp import ClientSession

# videos.db에서 video_id를 불러오는 함수
def get_video_ids_from_search_videos_list():
    db_folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "videos.db")
    conn = sqlite3.connect(db_folder_path)
    
    query = "SELECT id, activation FROM videos"  # video_id와 activation 값 가져오기
    video_ids_df = pd.read_sql(query, conn)
    
    conn.close()
    return video_ids_df

# 비동기 함수로 API 요청을 수행
async def fetch_video_data(session: ClientSession, video_id: str, API_KEY: str):
    url = f'https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_id}&key={API_KEY}'
    
    try:
        async with session.get(url) as response:
            if response.status != 200:
                logging.error(f"API 호출 실패: 상태 코드 {response.status}")
                return None
            
            data = await response.json()
            return data
    except Exception as e:
        logging.error(f"API 호출 중 오류 발생: {e}")
        return None

# 비동기 세션 수집 함수
async def collect_video_sessions(video_data, API_KEY, conn):
    async with ClientSession() as session:
        tasks = []
        
        # video_data의 각 row에 대해 비동기 요청 작업을 생성
        for index, row in video_data.iterrows():
            video_id = row['id']
            activation = row['activation']
            tasks.append(fetch_video_data(session, video_id, API_KEY))

        # 모든 작업을 비동기적으로 실행하고 결과를 기다림
        responses = await asyncio.gather(*tasks)

        # 결과 처리
        cursor = conn.cursor()
        for video_id, response in zip(video_data['id'], responses):
            if response is None:
                continue  # API 호출 실패 시 다음으로 넘어감

            try:
                video_info = response['items'][0]
                view_count = int(video_info['statistics']['viewCount'])
                like_count = int(video_info['statistics']['likeCount'])
                dislike_count = int(video_info['statistics'].get('dislikeCount', 0))
                comment_count = int(video_info['statistics']['commentCount'])
                collected_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # 영상별 세션 데이터 수 확인
                query = "SELECT COUNT(*) FROM videos_sessions WHERE video_id = ?"
                cursor.execute(query, (video_id,))
                session_count = cursor.fetchone()[0]

                # 조건 1: 영상별로 세션 데이터가 2개 이하인 경우
                if session_count < 2:
                    cursor.execute('''
                        INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at, activation)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at, activation))
                else:
                    # 조건 3: 최근 세션 데이터 2개를 가져옴
                    query = "SELECT view_count FROM videos_sessions WHERE video_id = ? ORDER BY collected_at DESC LIMIT 2"
                    cursor.execute(query, (video_id,))
                    recent_sessions = cursor.fetchall()

                    if len(recent_sessions) == 2:
                        previous_view_count_1 = recent_sessions[0][0]
                        previous_view_count_2 = recent_sessions[1][0]
                        increase = previous_view_count_1 - previous_view_count_2

                        ten_percent_of_current = int(view_count * 0.1)

                        # 조건 3: 상승폭이 현재 조회수의 10% 이하인 경우, activation 값을 0으로 업데이트
                        if increase <= ten_percent_of_current:
                            query = "UPDATE videos_sessions SET activation = 0 WHERE video_id = ?"
                            cursor.execute(query, (video_id,))
                        else:
                            cursor.execute('''
                                INSERT INTO videos_sessions (video_id, view_count, like_count, dislike_count, comment_count, collected_at, activation)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (video_id, view_count, like_count, dislike_count, comment_count, collected_at, activation))

            except Exception as e:
                logging.error(f"예상치 못한 오류 발생: {e}")

        # 변경 사항을 저장
        conn.commit()

# search_videos_sessions 비동기 처리 메인 함수
async def search_videos_sessions():
    API_KEY = 'AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM'
    
    video_data = get_video_ids_from_search_videos_list()
    
    # SQLite DB 연결
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', "videos_sessions.db"))
    
    # 테이블 생성
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos_sessions (
            video_id TEXT,
            view_count INTEGER,
            like_count INTEGER,
            dislike_count INTEGER,
            comment_count INTEGER,
            collected_at TEXT,
            activation INTEGER
        )
    ''')

    # 비동기 세션 수집 실행
    await collect_video_sessions(video_data, API_KEY, conn)

    # DB 연결 종료
    conn.close()

# 함수 실행
def run_search_videos_sessions():
    asyncio.run(search_videos_sessions())
