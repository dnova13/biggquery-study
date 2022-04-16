from dotenv import load_dotenv
from google.cloud import bigquery
import os

load_dotenv()

PROJECT = os.environ.get("PROJECT")
bq = bigquery.Client(project=PROJECT)

print(vars(bq))
print("--------------------")
print(dir(bq))
