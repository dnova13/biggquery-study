from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition, CreateDisposition
import pandas as pd  # pip install pandas
from dotenv import load_dotenv

# pip install google-cloud-bigquery

import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

# 판다스 데이터 프레임을 빅쿼리 테이블로 로드.
data = [
    (1, u'What is BigQuery?'),
    (2, u'Query essentials!!!'),
]

df = pd.DataFrame(data, columns=['chapter', 'title'])

print(df)

table_id = '{}.ch05.temp_table3'.format(PROJECT)

# # 빅쿼리로 데이터 로드
job = bq.load_table_from_dataframe(df, table_id)
job.result()  # blocks and waits

print("Loaded {} rows into {}".format(job.output_rows, table_id))
print('\n')

# 데이터 로드 옵션 지정.
load_config = LoadJobConfig(
    create_disposition=CreateDisposition.CREATE_IF_NEEDED,
    write_disposition=WriteDisposition.WRITE_TRUNCATE)

# # 빅쿼리로 데이터 로드
table_id = '{}.ch05.temp_table3-1'.format(PROJECT)

job = bq.load_table_from_dataframe(df, table_id, job_config=load_config)
job.result()  # blocks and waits

# print(vars(job))
# print(dir(job))
# print(job.project)
print(job.destination)

print("Loaded {} rows into {}".format(job.output_rows, job.destination))
