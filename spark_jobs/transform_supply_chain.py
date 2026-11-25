import io
import json
import os
import tempfile
from datetime import datetime
import boto3
from botocore.client import Config
import pandas as pd
import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

ingest_date = datetime.now().strftime("%Y-%m-%d")

s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9010',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadminpassword',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

spark = SparkSession.builder \
    .appName("SupplyChainETL") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

with tempfile.TemporaryDirectory() as tmpdir:
    files = ['sc_customers.csv', 'sc_products.csv', 'sc_shipments.csv']
    for fname in files:
        s3_key = f"postgres/ingest_date={ingest_date}/{fname}"
        s3.download_file('raw-data', s3_key, os.path.join(tmpdir, fname))

    rates_obj = s3.get_object(Bucket='raw-data', Key=f"api/exchange_rates/ingest_date={ingest_date}/rates.json")
    rates_json = json.loads(rates_obj['Body'].read().decode('utf-8'))

    rates_rows = []
    for date_str, curr_map in rates_json.get("rates", {}).items():
        if "IDR" in curr_map:
            rates_rows.append({
                "rate_date": date_str,
                "base_currency": "USD",
                "target_currency": "IDR",
                "exchange_rate": float(curr_map["IDR"])
            })

    rates_pdf = pd.DataFrame(rates_rows)
    rates_pdf['rate_date'] = pd.to_datetime(rates_pdf['rate_date'])
    full_idx = pd.date_range(start='2014-12-01', end=datetime.now().strftime('%Y-%m-%d'), freq='D')
    rates_filled = (
        rates_pdf.set_index('rate_date')
        .reindex(full_idx)
        .ffill()
        .bfill()
        .reset_index()
        .rename(columns={'index': 'rate_date'})
    )
    rates_filled['base_currency'] = 'USD'
    rates_filled['target_currency'] = 'IDR'
    rates_filled['rate_date_str'] = rates_filled['rate_date'].dt.strftime('%Y-%m-%d')

    df_cust_raw = spark.read.csv(os.path.join(tmpdir, 'sc_customers.csv'), header=True, inferSchema=True)
    df_prod_raw = spark.read.csv(os.path.join(tmpdir, 'sc_products.csv'), header=True, inferSchema=True)
    df_ship_raw = spark.read.csv(os.path.join(tmpdir, 'sc_shipments.csv'), header=True, inferSchema=True)

    df_cust_clean = df_cust_raw \
        .filter(F.col("customer_id").isNotNull()) \
        .withColumn("full_name", F.trim(F.concat_ws(" ", F.col("customer_fname"), F.col("customer_lname")))) \
        .withColumn("customer_segment", F.initcap(F.trim(F.col("customer_segment")))) \
        .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city")))) \
        .withColumn("customer_country", F.trim(F.col("customer_country"))) \
        .withColumn("updated_at", F.current_timestamp()) \
        .select("customer_id", "full_name", "customer_segment", "customer_city", "customer_country", "updated_at") \
        .dropDuplicates(["customer_id"])

    df_prod_clean = df_prod_raw \
        .filter(F.col("product_card_id").isNotNull()) \
        .withColumn("product_name", F.trim(F.col("product_name"))) \
        .withColumn("category_name", F.trim(F.col("category_name"))) \
        .withColumn("department_name", F.trim(F.col("department_name"))) \
        .withColumn("product_price", F.col("product_price").cast(DoubleType())) \
        .withColumn("updated_at", F.current_timestamp()) \
        .select("product_card_id", "product_name", "category_name", "department_name", "product_price", "updated_at") \
        .dropDuplicates(["product_card_id"])

    df_rates_spark = spark.createDataFrame(rates_filled[['rate_date_str', 'exchange_rate']])

    df_ship_clean = df_ship_raw \
        .filter(F.col("order_item_id").isNotNull() & F.col("order_date").isNotNull() & (F.col("sales") >= 0)) \
        .withColumn("order_date_clean", F.to_date(F.col("order_date"))) \
        .withColumn("shipping_date_clean", F.to_date(F.col("shipping_date"))) \
        .withColumn("order_date_str", F.date_format(F.col("order_date_clean"), "yyyy-MM-dd")) \
        .withColumn("shipping_delay_days", F.col("days_for_shipping_real") - F.col("days_for_shipment_scheduled")) \
        .withColumn("is_late_delivery", F.when(F.col("late_delivery_risk") == 1, 1).otherwise(0)) \
        .join(df_rates_spark, F.col("order_date_str") == F.col("rate_date_str"), "left") \
        .withColumn("sales_usd", F.round(F.col("sales").cast(DoubleType()), 2)) \
        .withColumn("sales_idr", F.round(F.col("sales_usd") * F.coalesce(F.col("exchange_rate"), F.lit(15500.0)), 2)) \
        .withColumn("profit_usd", F.round(F.col("order_profit_per_order").cast(DoubleType()), 2)) \
        .withColumn("updated_at", F.current_timestamp()) \
        .select(
            "order_item_id",
            "order_id",
            "customer_id",
            "product_card_id",
            F.col("order_date_clean").alias("order_date"),
            F.col("shipping_date_clean").alias("shipping_date"),
            F.col("days_for_shipping_real").alias("days_shipping_real"),
            F.col("days_for_shipment_scheduled").alias("days_shipment_scheduled"),
            "shipping_delay_days",
            F.col("delivery_status"),
            "is_late_delivery",
            F.col("shipping_mode"),
            F.col("market"),
            F.col("order_region"),
            F.col("order_country"),
            F.col("order_item_quantity").alias("quantity"),
            "sales_usd",
            "sales_idr",
            "profit_usd",
            "updated_at"
        ) \
        .dropDuplicates(["order_item_id"])

    cust_out = df_cust_clean.toPandas()
    prod_out = df_prod_clean.toPandas()
    ship_out = df_ship_clean.toPandas()

    for name, pdf in [("dim_customers", cust_out), ("dim_products", prod_out), ("fact_shipments", ship_out)]:
        parquet_buf = io.BytesIO()
        pdf.to_parquet(parquet_buf, index=False)
        curated_key = f"supply_chain/ingest_date={ingest_date}/{name}.parquet"
        s3.put_object(Bucket='curated-data', Key=curated_key, Body=parquet_buf.getvalue())
        print(f"[RustFS Curated] {len(pdf)} baris disimpan ke s3://curated-data/{curated_key}")

    ch_client = clickhouse_connect.get_client(host='localhost', port=8123)

    rates_dwh = rates_filled[['rate_date', 'base_currency', 'target_currency', 'exchange_rate']].copy()
    rates_dwh['rate_date'] = rates_dwh['rate_date'].dt.date
    rates_dwh['ingested_at'] = datetime.now()
    ch_client.insert_df('dwh_supply_chain.dim_exchange_rates', rates_dwh)

    ch_client.insert_df('dwh_supply_chain.dim_customers', cust_out)
    ch_client.insert_df('dwh_supply_chain.dim_products', prod_out)

    ship_out['order_date'] = pd.to_datetime(ship_out['order_date']).dt.date
    ship_out['shipping_date'] = pd.to_datetime(ship_out['shipping_date']).dt.date
    ch_client.insert_df('dwh_supply_chain.fact_shipments', ship_out)

    print(f"[ClickHouse DWH] Berhasil memuat {len(ship_out)} baris fakta & seluruh dimensi ke 'dwh_supply_chain'!")

spark.stop()