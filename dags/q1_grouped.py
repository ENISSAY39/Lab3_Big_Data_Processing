from __future__ import annotations

from datetime import datetime
from time import sleep

from airflow.decorators import dag, task, task_group


@dag(
    dag_id="q1_grouped",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["lab3"],
)
def q1_grouped():

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
    def load(results):
        print(results[0])
        print(results[1])
        print("loaded")

    @task_group(group_id="transforms")
    def transforms():
        return [transform_a(), transform_b()]

    e = extract()

    results = transforms()

    l = load(results)

    e >> results >> l


q1_grouped()