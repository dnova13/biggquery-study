
import pandas as pd  # pip install pandas
import time
import gzip
from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition, CreateDisposition
from dotenv import load_dotenv

# pip install google-cloud-bigquery

import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

# 로컬 파일을 로드
job_config = bigquery.LoadJobConfig()
job_config.autodetect = True
job_config.source_format = bigquery.SourceFormat.CSV
job_config.null_marker = 'NULL'

table_id = f'{PROJECT}.ch05.college_scorecard_1'

with gzip.open('../ch04/college_scorecard.csv.gz') as fp:
    job = bq.load_table_from_file(fp, table_id, job_config=job_config)

job.result()
table = bq.get_table(job.destination)
print("Loaded {} rows into {}.".format(table.num_rows, table.table_id))
