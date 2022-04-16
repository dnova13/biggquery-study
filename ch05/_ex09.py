import os

# pip install google-cloud-bigquery
from google.cloud import bigquery

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

table_id = 'bigquery-public-data.london_bicycles.cycle_stations'

# ex1
table = bq.get_table(table_id)
rows1 = bq.list_rows(table,
                     start_index=0,
                     max_results=5)

print([r for r in rows1])

# ex2
page_size = 10000
row_iter = bq.list_rows(table,
                        page_size=page_size)

for page in row_iter.pages:
    rows = list(page)
    # 로드된 행에 대해 필요한 작업을 실행한다.
    print(len(rows))


# ex3
fields = [
    field for field in table.schema if 'count' in field.name or field.name == 'id']
rows2 = bq.list_rows(table,
                     start_index=300,
                     max_results=5,
                     selected_fields=fields)

print([r for r in rows2])


# ex4
fmt = '{!s:<10} ' * len(rows2.schema)
print(fmt.format(*[field.name for field in rows2.schema]))
for row in rows2:
    print(fmt.format(*row))
