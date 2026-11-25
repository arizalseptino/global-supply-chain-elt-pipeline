import io
import json
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import boto3
from botocore.client import Config
import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator

S3_CONFIG = {
    'endpoint_url': 'http://rustfs:9000',
    'aws_access_key_id': 'minioadmin',
    'aws_secret_access_key': 'minioadminpassword',
    'config': Config(signature_version='s3v4'),
    'region_name': 'us-east-1'
}

def extract_postgres(**context):
    ingest_date = context['ds']
    pg_conn = psycopg2.connect(
        host="postgres-source",
        port=5432,
        database="ecommerce_oltp",
        user="admin",
        password="adminpassword"
    )
    s3 = boto3.client('s3', **S3_CONFIG)

    for table in ['sc_customers', 'sc_products', 'sc_shipments']:
        df = pd.read_sql(f"SELECT * FROM {table}", pg_conn)
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        s3.put_object(
            Bucket='raw-data',
            Key=f"postgres/ingest_date={ingest_date}/{table}.csv",
            Body=buf.getvalue()
        )
    pg_conn.close()

def extract_exchange_rates(**context):
    ingest_date = context['ds']
    url = "https://api.frankfurter.app/2015-01-01..?from=USD&to=IDR,EUR"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    s3 = boto3.client('s3', **S3_CONFIG)
    s3.put_object(
        Bucket='raw-data',
        Key=f"api/exchange_rates/ingest_date={ingest_date}/rates.json",
        Body=json.dumps(resp.json())
    )

def transform_and_load_dwh(**context):
    ingest_date = context['ds']
    s3 = boto3.client('s3', **S3_CONFIG)

    def read_csv_s3(table):
        obj = s3.get_object(Bucket='raw-data', Key=f"postgres/ingest_date={ingest_date}/{table}.csv")
        return pd.read_csv(io.BytesIO(obj['Body'].read()))

    df_cust = read_csv_s3('sc_customers')
    df_prod = read_csv_s3('sc_products')
    df_ship = read_csv_s3('sc_shipments')

    rates_obj = s3.get_object(Bucket='raw-data', Key=f"api/exchange_rates/ingest_date={ingest_date}/rates.json")
    rates_json = json.loads(rates_obj['Body'].read().decode('utf-8'))

    rates_rows = [
        {"rate_date": d, "base_currency": "USD", "target_currency": "IDR", "exchange_rate": float(v["IDR"])}
        for d, v in rates_json.get("rates", {}).items() if "IDR" in v
    ]
    rates_pdf = pd.DataFrame(rates_rows)
    rates_pdf['rate_date'] = pd.to_datetime(rates_pdf['rate_date'])
    full_idx = pd.date_range(start='2014-12-01', end=datetime.now().strftime('%Y-%m-%d'), freq='D')
    rates_filled = (
        rates_pdf.set_index('rate_date').reindex(full_idx).ffill().bfill().reset_index().rename(columns={'index': 'rate_date'})
    )
    rates_filled['base_currency'] = 'USD'
    rates_filled['target_currency'] = 'IDR'
    rates_filled['rate_date_str'] = rates_filled['rate_date'].dt.strftime('%Y-%m-%d')

    now_ts = datetime.now()

    df_cust = df_cust.dropna(subset=['customer_id']).drop_duplicates(subset=['customer_id']).copy()
    df_cust['full_name'] = (df_cust['customer_fname'].fillna('') + ' ' + df_cust['customer_lname'].fillna('')).str.strip()
    df_cust['customer_segment'] = df_cust['customer_segment'].fillna('').str.strip().str.title()
    df_cust['customer_city'] = df_cust['customer_city'].fillna('').str.strip().str.title()
    df_cust['customer_country'] = df_cust['customer_country'].fillna('').str.strip()
    df_cust['updated_at'] = now_ts
    dim_customers = df_cust[['customer_id', 'full_name', 'customer_segment', 'customer_city', 'customer_country', 'updated_at']]

    df_prod = df_prod.dropna(subset=['product_card_id']).drop_duplicates(subset=['product_card_id']).copy()
    df_prod['product_name'] = df_prod['product_name'].fillna('').str.strip()
    df_prod['category_name'] = df_prod['category_name'].fillna('').str.strip()
    df_prod['department_name'] = df_prod['department_name'].fillna('').str.strip()
    df_prod['product_price'] = df_prod['product_price'].astype(float)
    df_prod['updated_at'] = now_ts
    dim_products = df_prod[['product_card_id', 'product_name', 'category_name', 'department_name', 'product_price', 'updated_at']]

    df_ship = df_ship.dropna(subset=['order_item_id', 'order_date']).drop_duplicates(subset=['order_item_id']).copy()
    df_ship = df_ship[df_ship['sales'] >= 0]
    df_ship['order_date_dt'] = pd.to_datetime(df_ship['order_date'])
    df_ship['shipping_date_dt'] = pd.to_datetime(df_ship['shipping_date'])
    df_ship['rate_date_str'] = df_ship['order_date_dt'].dt.strftime('%Y-%m-%d')

    df_ship = df_ship.merge(rates_filled[['rate_date_str', 'exchange_rate']], on='rate_date_str', how='left')
    df_ship['exchange_rate'] = df_ship['exchange_rate'].fillna(15500.0)
    df_ship['shipping_delay_days'] = df_ship['days_for_shipping_real'] - df_ship['days_for_shipment_scheduled']
    df_ship['is_late_delivery'] = (df_ship['late_delivery_risk'] == 1).astype(int)
    df_ship['sales_usd'] = df_ship['sales'].round(2)
    df_ship['sales_idr'] = (df_ship['sales_usd'] * df_ship['exchange_rate']).round(2)
    df_ship['profit_usd'] = df_ship['order_profit_per_order'].round(2)
    df_ship['order_date'] = df_ship['order_date_dt'].dt.date
    df_ship['shipping_date'] = df_ship['shipping_date_dt'].dt.date
    df_ship['updated_at'] = now_ts

    fact_shipments = df_ship[[
        'order_item_id', 'order_id', 'customer_id', 'product_card_id',
        'order_date', 'shipping_date', 'days_for_shipping_real', 'days_for_shipment_scheduled',
        'shipping_delay_days', 'delivery_status', 'is_late_delivery',
        'shipping_mode', 'market', 'order_region', 'order_country',
        'order_item_quantity', 'sales_usd', 'sales_idr', 'profit_usd', 'updated_at'
    ]].rename(columns={
        'days_for_shipping_real': 'days_shipping_real',
        'days_for_shipment_scheduled': 'days_shipment_scheduled',
        'order_item_quantity': 'quantity'
    })

    for name, pdf in [('dim_customers', dim_customers), ('dim_products', dim_products), ('fact_shipments', fact_shipments)]:
        pbuf = io.BytesIO()
        pdf.to_parquet(pbuf, index=False)
        s3.put_object(
            Bucket='curated-data',
            Key=f"supply_chain/ingest_date={ingest_date}/{name}.parquet",
            Body=pbuf.getvalue()
        )

    ch_client = clickhouse_connect.get_client(host='host.docker.internal', port=8123)
    rates_dwh = rates_filled[['rate_date', 'base_currency', 'target_currency', 'exchange_rate']].copy()
    rates_dwh['rate_date'] = rates_dwh['rate_date'].dt.date
    rates_dwh['ingested_at'] = now_ts

    ch_client.insert_df('dwh_supply_chain.dim_exchange_rates', rates_dwh)
    ch_client.insert_df('dwh_supply_chain.dim_customers', dim_customers)
    ch_client.insert_df('dwh_supply_chain.dim_products', dim_products)
    ch_client.insert_df('dwh_supply_chain.fact_shipments', fact_shipments)

def validate_dwh_quality():
    ch_client = clickhouse_connect.get_client(host='host.docker.internal', port=8123)
    row_count = ch_client.command("SELECT count() FROM dwh_supply_chain.fact_shipments FINAL")
    null_idr = ch_client.command("SELECT count() FROM dwh_supply_chain.fact_shipments FINAL WHERE sales_idr <= 0")
    if row_count == 0 or null_idr > 0:
        raise ValueError(f"Data Quality Check Gagal! rows={row_count}, invalid_idr={null_idr}")
    print(f"[Data Quality Passed] Total baris valid di ClickHouse: {row_count}")

default_args = {
    'owner': 'arizal',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='supply_chain_end_to_end_elt',
    default_args=default_args,
    description='Automated Supply Chain ELT Pipeline (Postgres + REST API -> RustFS -> ClickHouse)',
    schedule_interval='@daily',
    start_date=datetime(2026, 11, 20),
    catchup=False,
    tags=['supply-chain', 'elt', 'clickhouse'],
) as dag:

    t_extract_pg = PythonOperator(
        task_id='extract_postgres_to_lake',
        python_callable=extract_postgres,
    )

    t_extract_api = PythonOperator(
        task_id='extract_api_to_lake',
        python_callable=extract_exchange_rates,
    )

    t_transform_load = PythonOperator(
        task_id='transform_and_load_dwh',
        python_callable=transform_and_load_dwh,
    )

    t_quality_check = PythonOperator(
        task_id='validate_dwh_quality',
        python_callable=validate_dwh_quality,
    )

    [t_extract_pg, t_extract_api] >> t_transform_load >> t_quality_check