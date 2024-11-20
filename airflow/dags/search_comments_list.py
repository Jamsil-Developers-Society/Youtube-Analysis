import os
from urllib import request
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
# textblob import TextBlob
import requests
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import logging
import os

def search_comments_list():
    #logger
    logger = logging.getLogger(' ')
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    #DB
    comments_DB = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','comments.db'))
    try:
        existing_sessions = pd.read_sql('SELECT comment_id FROM comments', comments_DB)
    except Exception:
        existing_sessions = pd.DataFrame(columns=['comment_id'])
    existing_comment_ids = set(existing_sessions['comment_id'])

    #url
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','videos.db'))
    # rows = pd.DataFrame(columns=['id', 'categoryId', 'publishedAt', 'channelId', 'title', 'description'])
    query = "SELECT id FROM videos"
    try:
        existing_videos = pd.read_sql(query, conn)
    except Exception:
        existing_videos = pd.DataFrame(columns=['id'])
    # logger.info(existing_videos)

    # 데이터 수집용 임시 코드
    # existing_videos = pd.DataFrame({'id':['uT9s4BXcv6w', 'I4Q1B-JCgSg', 'h5Zwg3Ag-bE', '1QAnXEQ3tBw', 'Xj0Jtjg3lHQ', 'Py9S2bD0RZQ', 'L7nuMgfeynw', 'bn0Kh9c4Zv4', 'RMprlMfsHAU', 'Yg1WMwotiSU', 'BWtBckf8RIw', 'BF08okjPkBY', 'WckEFzGku0Q', 'rAD8sA9UgFo', 'h-2LIjOt0rA', 'Twn_4AW0M6U', 'dBIWoBpWnvQ', 'bB3-CUMERIU', 'Aja2KfuoqGA']})

    comments_list = pd.DataFrame(columns=['video_id', 'comment_id', 'text', 'author', 'published_at', 'like_count', 'parent_comment_id', 'sentiment'])

    for video_id in existing_videos['id']:
        url = f"https://www.googleapis.com/youtube/v3/commentThreads?key=AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM&part=id,snippet&videoId={video_id}"
        logging.info(url)
        bool = True

        try:
            while bool:
                response = requests.get(url).json()
                # logging.info(response)
                array_items = response["items"] # if 'items' in response else response['pageInfo']['items']
                for item in array_items:
                    comment_data = item["snippet"]["topLevelComment"]["snippet"]
                    comment_id = item["snippet"]["topLevelComment"]["id"]
                    parent_id = None  # 최상위 댓글이므로 부모 ID는 None

                    if comment_id not in existing_comment_ids:
                        comment = {
                            "video_id": video_id,
                            "comment_id": comment_id,
                            "text": comment_data["textOriginal"],
                            "author": comment_data["authorDisplayName"],
                            "published_at": comment_data["publishedAt"],
                            "like_count": comment_data["likeCount"],
                            #"dislike_count":comment_data["dislikeCount"],
                            "parent_comment_id": parent_id,
                            "sentiment": None
                        }
                        comments_list = pd.concat([comments_list, pd.DataFrame([comment])], ignore_index=True)
                        existing_sessions = pd.concat([existing_sessions, pd.DataFrame([{"comment_id": comment_id,}])], ignore_index=True)

                    # 대댓글이 있는 경우 처리              
                    # if item["snippet"]["totalReplyCount"] > 0:
                    #     reply_request = youtube.comments().list(
                    #         part="snippet",
                    #         parentId=comment_id,
                    #         maxResults=100
                    #     )
                    #     reply_response = reply_request.execute()
                    #     for reply_item in reply_response["items"]:
                    #         reply_data = reply_item["snippet"]
                    #         reply_comment = {
                    #             "video_id": video_id,
                    #             "comment_id": reply_item["id"],
                    #             "text": reply_data["textOriginal"],
                    #             "author": reply_data["authorDisplayName"],
                    #             "published_at": reply_data["publishedAt"],
                    #             "like_count": reply_data["likeCount"],
                    #             #"dislike_count":reply_data["dislikeCount"],
                    #             "parent_comment_id": comment_id,
                    #             "sentiment": None
                    #         }
                    #         comments_list.append(reply_comment)

                if "nextPageToken" in response:
                    url = f"https://www.googleapis.com/youtube/v3/commentThreads?key=AIzaSyAi2Zm9LEW2z3dJJbfgtC-V8eAQw0trnqM&part=id,snippet&videoId={video_id}&pageToken={response['nextPageToken']}"
                else:
                    bool = False
        except Exception as e:
            logging.error(f'오류 메세지 : {e}')
            logging.error(f'response : {response}')

    logging.info(comments_list)
    if len(comments_list) > 0:
        comments_list.drop_duplicates(subset=['comment_id'])
        comments_list.to_sql('comments', comments_DB, if_exists='append', index=False)
        logger.info(f"{len(comments_list)}개의 비디오 데이터를 추가했습니다. ")
    else:
        logger.info("새로 추가할 비디오 데이터가 없습니다.")
        
    return 0

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'search_comments_list',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='search_comments_list',
    python_callable=search_comments_list,
    dag=dag
)