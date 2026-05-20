from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="q1_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["lab3"],
)
def q1_pipeline():

    @task.bash
    def print_date():
        return "date"

    @task.bash
    def whoami():
        return "whoami"

    @task.bash
    def count_dags():
        return "find /opt/airflow/dags -name '*.py' | wc -l"

    @task.bash
    def done():
        return 'echo "Lab 3 - Part 1 complete"'

    a = print_date()
    b = whoami()
    c = count_dags()
    d = done()

    a >> [b, c] >> d


q1_pipeline()
