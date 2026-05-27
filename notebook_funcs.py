"""Extract, transform, aggregate functions for the data pipeline."""
import boto3
import pandas as pd
from io import BytesIO
from datetime import date
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import os
import logging
import sys

load_dotenv()

# Configuration
AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION            = os.getenv('AWS_REGION', 'us-east-1')
BUCKET                = os.getenv('BUCKET')

_REQUIRED = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'BUCKET']
_missing = [v for v in _REQUIRED if not os.getenv(v)]
if _missing:
    sys.exit(f"Missing required env vars: {', '.join(_missing)}")

# S3 client
s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# Logger
log = logging.getLogger('pipeline')

_RETRY = dict(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10), reraise=True)


@retry(**_RETRY)
def extract(local_path: str = 'data/local_orders.csv',
            ingest_date: str | None = None) -> str:
    """Read raw CSV and land it in bronze, partitioned by ingest_date.
    Returns the bronze S3 key."""
    ingest_date = ingest_date or date.today().isoformat()
    df = pd.read_csv(local_path)
    key = f'bronze/orders/ingest_date={ingest_date}/orders.csv'
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=df.to_csv(index=False).encode('utf-8'))
    log.info('bronze: %d rows → %s', len(df), key)
    return key


@retry(**_RETRY)
def transform(bronze_key: str) -> list[str]:
    """Read bronze, clean & type, write Parquet partitioned by order_date.
    Returns list of silver keys written."""
    obj = s3.get_object(Bucket=BUCKET, Key=bronze_key)
    raw = pd.read_csv(BytesIO(obj['Body'].read()))
    before = len(raw)

    silver = raw.copy()
    silver['customer'] = silver['customer'].astype(str).str.strip()
    silver['status']   = silver['status'].astype(str).str.strip()
    silver = silver[silver['status'] != '']
    silver = silver[silver['price'].notna() & (silver['price'] != '')]
    silver['price']      = silver['price'].astype(float)
    silver['order_date'] = pd.to_datetime(silver['order_date']).dt.date

    valid  = {'pending', 'in_transit', 'delivered', 'cancelled'}
    silver = silver[silver['status'].isin(valid)]
    silver = silver.rename(columns={
        'customer': 'customer_name',
        'pickup':   'pickup_addr',
        'dropoff':  'dropoff_addr',
        'price':    'price_mnt',
    })

    written = []
    for order_date, group in silver.groupby('order_date'):
        key = f'silver/orders/order_date={order_date}/orders.parquet'
        buf = BytesIO()
        group.drop(columns=['order_date']).to_parquet(
            buf, index=False, engine='pyarrow', compression='snappy')
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        written.append(key)

    log.info('transform: %d → %d rows; %d silver partitions written',
             before, len(silver), len(written))
    return written


@retry(**_RETRY)
def aggregate() -> str:
    """Read all silver partitions, build daily revenue gold, return key."""
    paginator = s3.get_paginator('list_objects_v2')
    keys = [
        o['Key']
        for page in paginator.paginate(Bucket=BUCKET, Prefix='silver/orders/')
        for o in page.get('Contents', [])
        if o['Key'].endswith('.parquet')
    ]

    if not keys:
        log.warning('aggregate: no silver partitions found — gold not written')
        return ''

    frames = []
    for k in keys:
        d   = k.split('order_date=')[1].split('/')[0]
        obj = s3.get_object(Bucket=BUCKET, Key=k)
        df  = pd.read_parquet(BytesIO(obj['Body'].read()))
        df['order_date'] = pd.to_datetime(d).date()
        frames.append(df)
    silver_all = pd.concat(frames, ignore_index=True)

    daily = (silver_all[silver_all['status'] == 'delivered']
             .groupby('order_date', as_index=False)
             .agg(orders_delivered=('order_id', 'count'),
                  revenue_mnt=('price_mnt', 'sum'))
             .sort_values('order_date'))

    out_key = 'gold/daily_revenue/revenue.parquet'
    buf = BytesIO()
    daily.to_parquet(buf, index=False, engine='pyarrow', compression='snappy')
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue())
    log.info('aggregate: gold rows: %d → %s', len(daily), out_key)
    return out_key
