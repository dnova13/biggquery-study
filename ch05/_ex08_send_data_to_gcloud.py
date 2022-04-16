import os

# pip install google-cloud-bigquery
from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition, CreateDisposition
from dotenv import load_dotenv

load_dotenv()

PROJECT = os.environ.get("PROJECT")
BUCKET = "fdsfdsddd3288"
bq = bigquery.Client(project=PROJECT)

# 빅쿼리 데이터를 구글 스토리지로 보냄.
source_tbl = 'bigquery-public-data.london_bicycles.cycle_stations'
dest_uri = 'gs://{}/tmp/exported/cycle_stations'.format(BUCKET)
config = bigquery.job.ExtractJobConfig(
    destination_format=bigquery.job.DestinationFormat.NEWLINE_DELIMITED_JSON)

# 주의사항 오직 같은 지역의 버킷으로 가능.
# 여기서 즉 EU 빅쿼리 데이터를 보내므로
# gsutil mb -l EU gs://<버킷이름> 또는 구글 클라우드에서 eu 로 버킷 생성해야함.
job = bq.extract_table(source_tbl, dest_uri,
                       location='EU', job_config=config)
job.result()  # 중단 및 대기
