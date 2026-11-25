import boto3
from botocore.client import Config
import psycopg2
import clickhouse_connect

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadminpassword',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

for bucket in ['raw-data', 'curated-data']:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"[RustFS] Bucket '{bucket}' siap.")
    except Exception as e:
        print(f"[RustFS] Info bucket '{bucket}': {e}")

pg_conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="ecommerce_oltp",
    user="admin",
    password="adminpassword"
)
pg_cur = pg_conn.cursor()

pg_cur.execute("""
DROP TABLE IF EXISTS orders, products, customers, sc_shipments, sc_products, sc_customers CASCADE;

CREATE TABLE sc_customers (
    customer_id INT PRIMARY KEY,
    customer_fname VARCHAR(100),
    customer_lname VARCHAR(100),
    customer_segment VARCHAR(50),
    customer_city VARCHAR(100),
    customer_country VARCHAR(100)
);

CREATE TABLE sc_products (
    product_card_id INT PRIMARY KEY,
    product_name VARCHAR(150),
    category_name VARCHAR(100),
    department_name VARCHAR(100),
    product_price NUMERIC(12, 2)
);

CREATE TABLE sc_shipments (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    customer_id INT REFERENCES sc_customers(customer_id),
    product_card_id INT REFERENCES sc_products(product_card_id),
    order_date TIMESTAMP,
    shipping_date TIMESTAMP,
    days_for_shipping_real INT,
    days_for_shipment_scheduled INT,
    delivery_status VARCHAR(50),
    late_delivery_risk INT,
    shipping_mode VARCHAR(50),
    market VARCHAR(50),
    order_region VARCHAR(100),
    order_country VARCHAR(100),
    order_item_quantity INT,
    sales NUMERIC(12, 2),
    order_profit_per_order NUMERIC(12, 2)
);
""")
pg_conn.commit()
pg_cur.close()
pg_conn.close()
print("[PostgreSQL] Tabel OLTP Supply Chain (sc_customers, sc_products, sc_shipments) siap.")

ch_client = clickhouse_connect.get_client(host='localhost', port=8123)

ch_client.command("DROP DATABASE IF EXISTS dwh_ecommerce")
ch_client.command("CREATE DATABASE IF NOT EXISTS dwh_supply_chain")

ch_client.command("""
CREATE TABLE IF NOT EXISTS dwh_supply_chain.dim_customers (
    customer_id UInt32,
    full_name String,
    customer_segment String,
    customer_city String,
    customer_country String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id
""")

ch_client.command("""
CREATE TABLE IF NOT EXISTS dwh_supply_chain.dim_products (
    product_card_id UInt32,
    product_name String,
    category_name String,
    department_name String,
    product_price Float64,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_card_id
""")

ch_client.command("""
CREATE TABLE IF NOT EXISTS dwh_supply_chain.dim_exchange_rates (
    rate_date Date,
    base_currency String,
    target_currency String,
    exchange_rate Float64,
    ingested_at DateTime
) ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (rate_date, base_currency, target_currency)
""")

ch_client.command("""
CREATE TABLE IF NOT EXISTS dwh_supply_chain.fact_shipments (
    order_item_id UInt32,
    order_id UInt32,
    customer_id UInt32,
    product_card_id UInt32,
    order_date Date,
    shipping_date Date,
    days_shipping_real Int32,
    days_shipment_scheduled Int32,
    shipping_delay_days Int32,
    delivery_status String,
    is_late_delivery UInt8,
    shipping_mode String,
    market String,
    order_region String,
    order_country String,
    quantity UInt32,
    sales_usd Float64,
    sales_idr Float64,
    profit_usd Float64,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, market, order_item_id)
""")

print("[ClickHouse] Database 'dwh_supply_chain' dan tabel Star Schema berhasil disiapkan.")