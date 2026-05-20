from datetime import datetime
import os
import getpass
from airflow.decorators import dag, task


@dag(
    dag_id="q1_taskflow",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["lab3", "q1"],
)
def q1_taskflow():

    @task
    def print_date() -> str:
        from datetime import datetime
        msg = str(datetime.now())
        print(msg)
        return msg

    @task
    def whoami() -> str:
        user = getpass.getuser()
        print(user)
        return user

    @task
    def count_dags() -> str:
        count = len(
            [f for f in os.listdir("/opt/airflow/dags") if f.endswith(".py")]
        )
        msg = f"{count} DAG files"
        print(msg)
        return msg

    @task
    def done() -> str:
        msg = "Lab 3 - Part 1 complete"
        print(msg)
        return msg

    date_task = print_date()
    whoami_task = whoami()
    count_task = count_dags()

    date_task >> [whoami_task, count_task] >> done()


q1_taskflow()