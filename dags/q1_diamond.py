from __future__ import annotations

from datetime import datetime
from time import sleep
import time

from airflow.decorators import dag, task

print(f"[BAD] Parsing q1_diamond at {time.strftime('%H:%M:%S')}")


@dag(
    dag_id="q1_diamond",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["lab3"],
)
def q1_diamond():

    @task.bash
    def extract():
        return 'echo "extracting..." && sleep 5'

    @task
    def transform_a():
        sleep(3)
        return "A done"

    @task
    def transform_b():
        sleep(7)
        return "B done"

    @task
    def load(a, b):
        print(a)
        print(b)
        print("loaded")

    e = extract()
    a = transform_a()
    b = transform_b()

    l = load(a, b)

    e >> [a, b] >> l


q1_diamond()
