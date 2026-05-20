from __future__ import annotations

from datetime import datetime

import duckdb

from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor

from include.duck import csv_to_parquet


@dag(
    dag_id="q2_ingest",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["lab3"],
)
def q2_ingest():

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="incoming/transactions_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=600,
        mode="reschedule",
    )

    @task
    def ingest(ds: str | None = None):

        csv_path = f"/opt/airflow/data/incoming/transactions_{ds}.csv"
        parquet_path = f"/opt/airflow/data/raw/transactions_{ds}.parquet"

        conn = duckdb.connect()

        count = conn.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{csv_path}')"
        ).fetchone()[0]

        print(f"Row count: {count}")

        csv_to_parquet(csv_path, parquet_path)

        print(f"Wrote parquet file: {parquet_path}")

    wait_for_file >> ingest()


q2_ingest()
