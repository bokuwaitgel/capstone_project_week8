# Orders Data Pipeline

A daily ETL pipeline that moves order data from a local CSV through bronze → silver → gold layers in S3.

## Architecture

```
data/local_orders.csv
        │
        ▼ extract()
bronze/orders/ingest_date=YYYY-MM-DD/orders.csv
        │
        ▼ transform()
silver/orders/order_date=YYYY-MM-DD/orders.parquet   (cleaned, typed, Snappy-compressed)
        │
        ▼ aggregate()
gold/daily_revenue/revenue.parquet                   (delivered orders, daily revenue totals)
```

## Local setup

```bash
cp .env.example .env        # fill in your AWS credentials and bucket name
pip install -r requirements.txt
python run_pipeline.py
```

Logs are written to both stdout and `pipeline.log`.

## Environment variables

| Variable               | Description                        |
|------------------------|------------------------------------|
| `AWS_ACCESS_KEY_ID`    | AWS access key                     |
| `AWS_SECRET_ACCESS_KEY`| AWS secret key                     |
| `AWS_REGION`           | AWS region (default: `us-east-1`)  |
| `BUCKET`               | S3 bucket name                     |

## CI / GitHub Actions

The workflow in [.github/workflows/daily.yml](.github/workflows/daily.yml) runs at **06:00 UTC** every day.
Store the four environment variables above as repository secrets (`Settings → Secrets → Actions`).
The pipeline log is uploaded as a workflow artifact (retained 30 days) on every run.

To trigger a manual run: **Actions → daily-pipeline → Run workflow**.

## Retry policy

Each stage (extract, transform, aggregate) retries up to **3 times** with exponential back-off (2 s → 10 s) on any transient AWS or I/O error.
