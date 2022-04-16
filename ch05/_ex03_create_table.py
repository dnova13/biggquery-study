from dotenv import load_dotenv

# pip install google-cloud-bigquery
from google.cloud import bigquery
import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

# 빈 테이블을 생성
table_id = '{}.ch05.temp_table'.format(PROJECT)
table = bq.create_table(table_id, exists_ok=True)

# 생성한 테이블 스키마 갱신 코드
schema = [
    bigquery.SchemaField("chapter", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
]

table_id = '{}.ch05.temp_table'.format(PROJECT)
table = bq.get_table(table_id)

print(table.etag)

table.schema = schema
table = bq.update_table(table, ["schema"])

print(table.schema)
print(table.etag)


# 테이블에 새 행을 추가하는 코드
rows = [
    (1, u'What is BigQuery?'),
    (2, u'Query essentials'),
]
errors = bq.insert_rows(table, rows)

# 잘못된 데이터를 테이블에 삽입했을때
# ('wont work', u'This will fail'), wont work 는 int 여야됨. 이거 삽입 안됨
rows = [
    ('3', u'Operating on data types'),
    ('wont work', u'This will fail'),
    ('4', u'Loading data into BigQuery'),
]
errors = bq.insert_rows(table, rows)
print(errors)

# 스키마를 미리 지정하여 테이블 생성
schema = [
    bigquery.SchemaField("chapter", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
]

table_id = '{}.ch05.temp_table2'.format(PROJECT)
table = bigquery.Table(table_id, schema)
table = bq.create_table(table, exists_ok=True)

print('{} created on {}'.format(table.table_id, table.created))
print(table.schema)
