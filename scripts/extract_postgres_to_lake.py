import io
from datetime import datetime
import pandas as pd
import psycopg2
import boto3
from botocore.client import Config

ingest_date = datetime.now().strftime("%Y-%m-%d")

pg_conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="ecommerce_oltp",
    user="admin",
    password="adminpassword"
)

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadminpassword',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

tables = ['sc_customers', 'sc_products', 'sc_shipments']

for table in tables:
    print(f"[Extract PG] Menarik tabel '{table}'...")
    df = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_key = f"postgres/ingest_date={ingest_date}/{table}.csv"
    s3.put_object(
        Bucket='raw-data',
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    print(f"[RustFS] Berhasil mengunggah {len(df)} baris ke s3://raw-data/{s3_key}")

pg_conn.close()