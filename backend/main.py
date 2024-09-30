import uvicorn
from fastapi import FastAPI
import os
from jinja2 import Template, Environment, FileSystemLoader
import sqlite3
import pandas as pd
import logging

app = FastAPI()

env = Environment(loader=FileSystemLoader('.'))

@app.get("/videos")
async def search_videos():
    template = env.get_template('sql/select_videos.sql')

    query = template.render()

    # logging.info(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),"..",'airflow','db','videos.db'))
    # result = pd.read_sql(query, conn)
    
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    result = pd.read_sql(query, conn)

    return result.to_json()

@app.get("/video/{video_id}")
async def search_video(video_id:int=0):
    template = env.get_template('sql/select_video.sql')

    query = template.render(video_id=video_id)
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
    result = pd.read_sql(query, conn)

    return result.to_json()

@app.get("/sessions/{video_id}")
async def search_video_sessions(video_id:int=0):
    template = env.get_template('sql/select_videos_sessions.sql')

    query = template.render(video_id=video_id)
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos_sessions.db'))
    result = pd.read_sql(query, conn)

    return result.to_json()

# @app.get("/comments/{video_id}")
# async def search_comments(video_id:int=0):
#     template = env.get_template('sql/select_video.sql')

#     query = template.render(video_id=video_id)
#     conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'videos.db'))
#     result = pd.read_sql(query, conn)

#     return result.to_json()

@app.get("/categories")
async def search_video_sessions():
    template = env.get_template('sql/select_categories.sql')

    query = template.render()
    conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'airflow', 'db', 'categories.db'))
    result = pd.read_sql(query, conn)

    return result.to_json()

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)