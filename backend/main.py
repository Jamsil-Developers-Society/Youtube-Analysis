import uvicorn
from fastapi import FastAPI
import os
from jinja2 import Template, Environment, FileSystemLoader
import sqlite3
import pandas as pd
import logging
from fastapi.middleware.cors import CORSMiddleware
import json

app = FastAPI()

env = Environment(loader=FileSystemLoader('.'))

@app.get("/api/total")
async def search_videos_with_category_name():
    template1 = env.get_template('sql/select_videos.sql')
    template2 = env.get_template('sql/select_categories.sql')

    query1 = template1.render()
    query2 = template2.render()

    conn1 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    conn2 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'categories.db'))

    df1 = pd.read_sql(query1, conn1)
    df2 = pd.read_sql(query2, conn2)
    # df2.drop(columns='index',inplace=True)

    # logging.info(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # result = pd.read_sql(query, conn)
    
    result = df1.merge(df2, how="left", left_on="categoryId", right_on="category_id")
    result = result.groupby('category_title').size().reset_index(name='count')
    # result.rename(columns={"category_title": "id","count":"value"}, inplace=True)
    # result.to_csv("asdfg3.csv")

    return result.to_json(orient='records')

@app.get("/api/genrerate")
async def search_videos_sessions_with_view_rate():
    template1 = env.get_template('sql/select_videos.sql')
    template2 = env.get_template('sql/select_categories.sql')
    template3 = env.get_template('sql/select_sessions_genre_rate.sql')

    query1 = template1.render()
    query2 = template2.render()
    query3 = template3.render()

    conn1 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    conn2 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'categories.db'))
    conn3 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos_sessions.db'))

    df1 = pd.read_sql(query1, conn1)
    df2 = pd.read_sql(query2, conn2)
    df3 = pd.read_sql(query3, conn3)
    # df2.drop(columns='index',inplace=True)

    # logging.info(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # result = pd.read_sql(query, conn)
    
    result = df3.merge(df1, how="left", left_on="video_id", right_on="id").merge(df2, how='left', left_on='categoryId', right_on="category_id")
    # result.to_csv("asdfg3.csv")
    # result = result.groupby('category_title').agg(avg_view_count_difference=('view_count_growth_rate', 'mean')).reset_index()
    result = result.groupby('category_title').agg(avg_view_count_difference=('view_count_difference', 'sum')).reset_index()
    # result.rename(columns={"category_title": "id","count":"value"}, inplace=True)
    # result.to_csv("asdfg4.csv")

    return result.to_json(orient='records')

@app.get("/api/genreperiod")
async def search_videos_sessions_with_period():
    template1 = env.get_template('sql/select_videos.sql')
    template2 = env.get_template('sql/select_categories.sql')
    template3 = env.get_template('sql/select_sessions_genre_period.sql')

    query1 = template1.render()
    query2 = template2.render()
    query3 = template3.render()

    conn1 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    conn2 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'categories.db'))
    conn3 = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos_sessions.db'))

    df1 = pd.read_sql(query1, conn1)
    df2 = pd.read_sql(query2, conn2)
    df3 = pd.read_sql(query3, conn3)
    # df2.drop(columns='index',inplace=True)

    # logging.info(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # result = pd.read_sql(query, conn)
    
    result = df3.merge(df1, how="left", left_on="video_id", right_on="id").merge(df2, how='left', left_on='categoryId', right_on="category_id")
    # result = result.groupby('category_title').agg(avg_view_count_difference=('view_count_growth_rate', 'mean')).reset_index()
    result = result.groupby('category_title').agg(avg_view_count_difference=('view_count_growth_rate', 'mean')).reset_index()
    # result.rename(columns={"category_title": "id","count":"value"}, inplace=True)

    return result.to_json(orient='records')

@app.get("/api/videos")
async def search_videos():
    template = env.get_template('sql/select_videos_sessions.sql')

    query = template.render()

    # logging.info(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # result = pd.read_sql(query, conn)
    
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos_sessions.db'))
    result = pd.read_sql(query, conn)

    result.to_csv("asdfg4.csv")

    return result.to_json(orient='records')

@app.get("/api/video/{video_id}")
async def search_video(video_id:int=0):
    template = env.get_template('sql/select_video.sql')

    query = template.render(video_id=video_id)
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    result = pd.read_sql(query, conn)

    return result.to_json(orient='records')

@app.get("/api/sessions/{video_id}")
async def search_video_sessions(video_id:int=0):
    template = env.get_template('sql/select_videos_sessions.sql')

    query = template.render(video_id=video_id)
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos_sessions.db'))
    result = pd.read_sql(query, conn)

    return result.to_json(orient='records')

# @app.get("/api/comments/{video_id}")
# async def search_comments(video_id:int=0):
#     template = env.get_template('sql/select_video.sql')

#     query = template.render(video_id=video_id)
#     conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
#     result = pd.read_sql(query, conn)

#     return result.to_json(orient='records')

@app.get("/api/categories")
async def search_video_sessions():
    template = env.get_template('sql/select_categories.sql')

    query = template.render()
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'categories.db'))
    result = pd.read_sql(query, conn)

    return result.to_json(orient='records')

# CORS 설정 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 모든 출처 허용 (보안 문제 때문에 필요한 출처만 허용하는 것이 좋습니다)
    allow_credentials=True,
    allow_methods=["*"],  # 모든 HTTP 메서드 허용 (GET, POST 등)
    allow_headers=["*"],  # 모든 헤더 허용
)

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)