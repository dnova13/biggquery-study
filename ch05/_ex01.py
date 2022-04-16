from dotenv import load_dotenv

# pip install google-cloud-bigquery
from google.cloud import bigquery
import os

# read_dotenv()
load_dotenv()

PROJECT = os.environ.get("PROJECT")

# 이거 둘다 됨. 일단 gcli 에서 project 디폴트 설정을 해서.
bq = bigquery.Client(project=PROJECT)
bq = bigquery.Client()

# [project].[datset] 명시 햇을 경우
# dsinfo = bq.get_dataset('bigquery-public-data.london_bicycles')

# 데이터셋 만 햇을 경우, 이때 기본 프로젝트 설정햇을 경우 가능.
dsinfo = bq.get_dataset('ch04')

print(dsinfo)
print(vars(dsinfo))
print(f'{dsinfo.dataset_id}, {dsinfo.created}, {dsinfo.location}', '\n')

# 데이터 셋의 접근 권한을 확인하는 코드
for access in dsinfo.access_entries:
    if access.role == 'READER':
        print(access)

# 데이터 셋 생성
dataset_id = "{}.ch05".format(PROJECT)
ds = bq.create_dataset(dataset_id, exists_ok=True)

# 다른지역에 데이터 셋 생성
dataset_id = "{}.ch05eu".format(PROJECT)
dsinfo = bigquery.Dataset(dataset_id)
dsinfo.location = 'EU'
ds = bq.create_dataset(dsinfo, exists_ok=True)

# 기본 프로젝트의 데이터 셋 삭제
# bq.delete_dataset('ch05', not_found_ok=True)

# 특정 프로젝트의 데이터셋을 삭제
# bq.delete_dataset('{}.ch05'.format('PROJECT'), not_found_ok=True)


# 데이터셋 정보를 수정.
dsinfo = bq.get_dataset("ch05")
print("dsinfo.description :", dsinfo.description)
dsinfo.description = "Chapter 5 of BigQuery: The Definitive Guide"
dsinfo = bq.update_dataset(dsinfo, ['description'])
print("dsinfo.description :", dsinfo.description)

# 데이터셋에 접근 권한을 부여
# 만약 안될경우 entity_id에서 구글 서비스 계정 iam 인증 받은 아이디인지 확인
dsinfo = bq.get_dataset("ch05")
entry = bigquery.AccessEntry(
    role="READER",
    entity_type="userByEmail",
    entity_id="dddid-673@dark-tenure-347003.iam.gserviceaccount.com",
)
if entry not in dsinfo.access_entries:
    entries = list(dsinfo.access_entries)
    entries.append(entry)

    print(entries)
    dsinfo.access_entries = entries
    dsinfo = bq.update_dataset(dsinfo, ["access_entries"])  # API request
else:
    print('{} already has access'.format(entry.entity_id))
print(dsinfo.access_entries, end='\n\n')

# 테이블 목록 출력하는 코드 "{project_id}.{dataset_id}"
tables = bq.list_tables("bigquery-public-data.london_bicycles")
tbls = bq.list_tables("ch04")

for table in tables:
    print(table.table_id)

for tbl in tbls:
    print(tbl.table_id)

print('\n')

# 테이블 행 수를 가져옴.
table = bq.get_table("bigquery-public-data.london_bicycles.cycle_stations")
print('{} rows in {}'.format(table.num_rows, table.table_id))
print('\n')

# 테이블에서 특정 이름을 가진 컬럼을 출력
table = bq.get_table("bigquery-public-data.london_bicycles.cycle_stations")
for field in table.schema:
    if 'count' in field.name:
        print(field)

print('\n')

table = bq.get_table("ch04.college_scorecard_etl")
for field in table.schema:
    if 'SAT_AVG' in field.name:
        print(field)


print('\n')
print('\n')
