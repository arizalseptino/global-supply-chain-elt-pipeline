import json
from datetime import datetime
import requests
import boto3
from botocore.client import Config

ingest_date = datetime.now().strftime("%Y-%m-%d")

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadminpassword',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

url = "https://api.frankfurter.app/2015-01-01..?from=USD&to=IDR,EUR"
print(f"[Extract API] Mengambil data kurs historis dari {url}...")

response = requests.get(url, timeout=30)
response.raise_for_status()
data = response.json()

s3_key = f"api/exchange_rates/ingest_date={ingest_date}/rates.json"
s3.put_object(
    Bucket='raw-data',
    Key=s3_key,
    Body=json.dumps(data)
)

total_days = len(data.get("rates", {}))
print(f"[RustFS] Berhasil mengunggah {total_days} hari data kurs ke s3://raw-data/{s3_key}")