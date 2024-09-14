import uvicorn
from fastapi import FastAPI
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from jinja2 import Template, Environment, FileSystemLoader

app = FastAPI()

env = Environment(loader=FileSystemLoader('.'))

def getCredentials():
    credentials = service_account.Credentials.from_service_account_file(os.path.join('json','data-infra-project-428914-95324759a136.json'))
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

@app.get("/videos")
async def search_videos():
    # return {"message": "Hello World"}
    

    # with open("sql/select_videos.sql", "r") as file:
    #     query = Template(file.read())

    template = env.get_template('sql/select_videos.sql')

    query = template.render(project='data-infra-project-428914', dataset='train', table='videos')

    client = getCredentials()

    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

@app.get("/video/{video_id}")
async def search_video(video_id:int=0):
    template = env.get_template('sql/select_video_id.sql')

    query = template.render(project='data-infra-project-428914', dataset='train', table='videos', video_id=video_id)

    client = getCredentials()

    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

@app.get("/sessions/{video_id}")
async def search_video_sessions(video_id:int=0):
    template = env.get_template('sql/select_video_id.sql')

    query = template.render(project='data-infra-project-428914', dataset='train', table='videos', video_id=video_id)

    client = getCredentials()

    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

@app.get("/comments/{video_id}")
async def search_comments(video_id:int=0):
    template = env.get_template('sql/select_video_id.sql')

    query = template.render(project='data-infra-project-428914', dataset='train', table='comments', video_id=video_id)

    client = getCredentials()

    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()

if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)