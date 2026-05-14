"""
broken_dag.py

Background story (for Part 4 of the lab):
    A junior data engineer wrote this DAG in a hurry before their summer holiday.
    It "works" - it produces output - but in two weeks it has already caused three
    production incidents:
        - The scheduler started lagging behind by 5 minutes every parsing cycle.
        - The metadata DB grew by 4 GB in a week.
        - One re-run after a server reboot duplicated half of yesterday's revenue.

Your job (Q4.1):
    Read this file carefully. Find EXACTLY FIVE distinct anti-patterns from CM3.
    Fill in the table in the lab subject (Q4.1) and produce a fixed version in
    `dags/q4_fixed.py` (Q4.2).

Do NOT trigger this DAG. Reading it is enough.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


# Pulled from a public config service at file load time -- "convenient" so
# the latest config is always picked up by every task automatically.
# (Hint: how often does Airflow load this file?)
try:
    CONFIG = requests.get(
        "https://httpbin.org/json",
        timeout=5,
    ).json()
except Exception:
    CONFIG = {}  # do not let a flaky network break parsing

INCOMING_DIR = "/opt/airflow/data/incoming"
OUTPUT_DIR = "/opt/airflow/data/raw"


def big_transform(**context):
    """Read the day's CSV with pandas, clean it, return the resulting DataFrame
    so the next task can write it out. The CSV is ~10k rows today but the
    business team plans to scale this up to millions of rows per day."""
    ds = context["ds"]
    csv_path = f"{INCOMING_DIR}/transactions_{ds}.csv"
    df = pd.read_csv(csv_path)
    df = df.dropna()
    df["amount_eur"] = df["amount_eur"].astype(float)
    df = df[df["amount_eur"] > 0]
    df["processed_at"] = datetime.now().isoformat()
    return df  # next task pulls it from XCom


def write_to_parquet(**context):
    """Write the DataFrame produced by `big_transform` to a single growing
    Parquet file. Append mode keeps history compact."""
    df = context["ti"].xcom_pull(task_ids="big_transform")
    out_path = f"{OUTPUT_DIR}/all_transactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    if os.path.exists(f"{OUTPUT_DIR}/all_transactions.parquet"):
        existing = pd.read_parquet(f"{OUTPUT_DIR}/all_transactions.parquet")
        df = pd.concat([existing, df], ignore_index=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_parquet(f"{OUTPUT_DIR}/all_transactions.parquet", index=False)
    print(f"Wrote {len(df)} rows. Config slug from upstream: {CONFIG.get('slideshow', {}).get('title')}")
    return out_path


default_args = {
    "owner": "intern_on_holiday",
    "depends_on_past": False,
    "email_on_failure": False,
}


with DAG(
    dag_id="broken_dag",
    description="Five anti-patterns -- DO NOT RUN, just read.",
    default_args=default_args,
    start_date=datetime(2026, 5, 10),
    schedule="@daily",
    catchup=False,
    tags=["lab3", "broken"],
) as dag:

    transform = PythonOperator(
        task_id="big_transform",
        python_callable=big_transform,
    )

    write = PythonOperator(
        task_id="write_to_parquet",
        python_callable=write_to_parquet,
    )

    transform >> write
