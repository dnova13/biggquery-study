from dotenv import load_dotenv

# pip install google-cloud-bigquery
from google.cloud import bigquery
import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

bq.delete_table('ch05.asd', not_found_ok=True)


# 삭제한 테이블 복구하는 명령어 [삭제한 테이블 명]@[timestamp]로 스냅샵이 잇어야 복원됨.
""" 
bq --location=US cp ch05.[삭제한 테이블 명]@[timestamp] ch05.[삭제한 테이블 명]
"""
# ex
""" 
bq --location=US cp ch05.asd@1418864998000 ch05.asd2
"""
