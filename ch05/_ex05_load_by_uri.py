from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition, CreateDisposition
import pandas as pd  # pip install pandas
from dotenv import load_dotenv
import time

# pip install google-cloud-bigquery

import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

# url(구글 클라우드 버켓) 통한 빅쿼리 데이터 로드
table_id = '{}.ch05.temp_table3-2'.format(PROJECT)

job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
job_config.source_format = bigquery.SourceFormat.CSV
job_config.null_marker = 'NULL'

uri = f"gs://{os.environ.get('BUCKET')}/college_scorecard.csv"
table_id = f'{PROJECT}.ch05.college_scorecard_1'

job = bq.load_table_from_uri(uri, table_id, job_config=job_config)
job.result()  # blocks and waits

while not job.done():
    print('.', end='', flush=True)
    time.sleep(0.5)

print('Done')

table = bq.get_table(job.destination)
print("Loaded {} rows into {}.".format(table.num_rows, table.table_id))
