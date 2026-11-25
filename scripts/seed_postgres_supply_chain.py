import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

print("[Seed] Membaca DataCoSupplyChainDataset.csv...")
df = pd.read_csv("data_raw/DataCoSupplyChainDataset.csv", encoding="latin1")

df['order_date_parsed'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
df['shipping_date_parsed'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')

customers_df = df[[
    'Customer Id', 'Customer Fname', 'Customer Lname',
    'Customer Segment', 'Customer City', 'Customer Country'
]].drop_duplicates(subset=['Customer Id']).fillna('')

products_df = df[[
    'Product Card Id', 'Product Name', 'Category Name',
    'Department Name', 'Product Price'
]].drop_duplicates(subset=['Product Card Id']).fillna({'Product Name': 'Unknown', 'Category Name': 'Unknown', 'Department Name': 'Unknown', 'Product Price': 0.0})

shipments_df = df[[
    'Order Item Id', 'Order Id', 'Customer Id', 'Product Card Id',
    'order_date_parsed', 'shipping_date_parsed',
    'Days for shipping (real)', 'Days for shipment (scheduled)',
    'Delivery Status', 'Late_delivery_risk', 'Shipping Mode',
    'Market', 'Order Region', 'Order Country',
    'Order Item Quantity', 'Sales', 'Order Profit Per Order'
]].drop_duplicates(subset=['Order Item Id'])

pg_conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="ecommerce_oltp",
    user="admin",
    password="adminpassword"
)
pg_cur = pg_conn.cursor()

execute_values(
    pg_cur,
    """
    INSERT INTO sc_customers (customer_id, customer_fname, customer_lname, customer_segment, customer_city, customer_country)
    VALUES %s ON CONFLICT (customer_id) DO NOTHING
    """,
    list(customers_df.itertuples(index=False, name=None)),
    page_size=5000
)
print(f"[PostgreSQL] {len(customers_df)} data master pelanggan dimuat.")

execute_values(
    pg_cur,
    """
    INSERT INTO sc_products (product_card_id, product_name, category_name, department_name, product_price)
    VALUES %s ON CONFLICT (product_card_id) DO NOTHING
    """,
    list(products_df.itertuples(index=False, name=None)),
    page_size=5000
)
print(f"[PostgreSQL] {len(products_df)} data master produk dimuat.")

shipment_records = []
for row in shipments_df.itertuples(index=False, name=None):
    row_list = list(row)
    row_list[4] = None if pd.isna(row_list[4]) else row_list[4].to_pydatetime()
    row_list[5] = None if pd.isna(row_list[5]) else row_list[5].to_pydatetime()
    shipment_records.append(tuple(row_list))

execute_values(
    pg_cur,
    """
    INSERT INTO sc_shipments (
        order_item_id, order_id, customer_id, product_card_id,
        order_date, shipping_date, days_for_shipping_real, days_for_shipment_scheduled,
        delivery_status, late_delivery_risk, shipping_mode, market,
        order_region, order_country, order_item_quantity, sales, order_profit_per_order
    )
    VALUES %s ON CONFLICT (order_item_id) DO NOTHING
    """,
    shipment_records,
    page_size=10000
)

pg_conn.commit()
pg_cur.close()
pg_conn.close()
print(f"[PostgreSQL] Selesai memuat {len(shipment_records)} transaksi pengiriman asli!")