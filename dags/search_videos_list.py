import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

import requests
import xml.etree.ElementTree as ET
import pandas as pd
import sqlite3
import logging

def insert_data_from_api():
    # 로거 설정
    logger = logging.getLogger(__name__)
    try:
        results = requests.get('https://api.vworld.kr/req/wfs?SERVICE=WFS&REQUEST=GetFeature&TYPENAME=lt_c_up201&PROPERTYNAME=mnum,sido_cd,sigungu_cd,dyear,dnum,ucode,bon_bun,bu_bun,uname,sido_name,sigg_name,ag_geom&VERSION=1.1.0&SRSNAME=EPSG:4019&OUTPUT=GML2&EXCEPTIONS=text/xml&KEY=3AE1D4E0-EBFB-3841-A4DE-EE254A31BD34&MAXFEATURES=1000')
        root = ET.fromstring(results.content)

        df = pd.DataFrame(columns=['year', 'sido_name', 'sigg_name', 'uname', 'coordinates'])

        # 네임스페이스
        namespaces = {
            'gml': 'http://www.opengis.net/gml',
            'sop': 'https://www.vworld.kr'  # 실제 네임스페이스 URL로 교체 필요
        }

        for feature in root.findall('.//gml:featureMember', namespaces=namespaces):
            dyear = feature.find('.//sop:dyear', namespaces=namespaces)
            sido_name = feature.find('.//sop:sido_name', namespaces=namespaces)
            sigg_name = feature.find('.//sop:sigg_name', namespaces=namespaces)
            uname = feature.find('.//sop:uname', namespaces=namespaces)
            coordinates = feature.find('.//sop:ag_geom/gml:MultiPolygon/gml:polygonMember/gml:Polygon/gml:outerBoundaryIs/gml:LinearRing/gml:coordinates', namespaces=namespaces)
            
            datarow = {
                'year': dyear.text,
                'sido_name': sido_name.text,
                'sigg_name': sigg_name.text,
                'uname': uname.text,
                'coordinates': coordinates.text,
            }
            df = df.append(datarow, ignore_index=True)

        # SQLite 데이터베이스 연결 생성(파일이 없으면 생성)
        conn = sqlite3.connect(os.path.join(os.path.dirname(os.path.dirname(__file__)),'db','main.db'))

        # 연결 속성 설정
        # conn.text_factory = str  # 기본값이 str이며, 이는 UTF-8로 인코딩됨을 의미합니다.

        # DataFrame을 SQL 테이블로 변환
        df.to_sql('location_data', conn, if_exists='replace', index=False)

        # 데이터베이스 연결 종료
        conn.close()
    except Exception as e:
        print(e)
    pass

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'insert_data_from_api',
    default_args=default_args,
    description='Export data from VWorld API, and insert to database',
    schedule_interval='@daily'
)

# PythonOperator로 태스크 설정
export_to_csv_task = PythonOperator(
    task_id='insert_data_from_api',
    python_callable=insert_data_from_api,
    dag=dag
)
