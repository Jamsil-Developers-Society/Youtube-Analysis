# 수정된 Dockerfile
# FROM apache/airflow:2.3.0
FROM apache/airflow:2.3.0-python3.9

# pip 업그레이드 (권장)
RUN pip install --upgrade pip

# 필요한 Python 패키지 설치
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    google-auth \
    google-cloud-bigquery \
    pandas \
    asyncio \
    aiohttp \
    jinja2 \
    python-dotenv \
    # googleapiclient \
    google-api-python-client \
    tensorflow \
    transformers \
    google_auth_oauthlib \
    tf-keras \
    protobuf==3.20.3
