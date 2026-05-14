"""
Example DAG - read this in Part 1 of the lab.

This is the ONLY complete DAG provided to you. Everything else you write yourself,
guided by the lab subject (PDF).

What this DAG does
------------------
Two simple tasks:
    say_hello       -> prints "Hello, EPF!"
    say_goodbye     -> prints "Goodbye, EPF!"

Run order (two complementary notations, same graph):
    1) TaskFlow: pass the upstream task object into the downstream call so the return
       value of ``say_hello`` becomes the ``previous`` argument of ``say_goodbye``.
    2) Classic Airflow: ``hello >> goodbye`` uses Python's bitshift tokens overloaded
       by Airflow to mean "run left before right" (you will use this a lot with
       ``BashOperator`` in ``q1_pipeline``).

Try it
------
1. Make sure your Airflow stack is up (`docker compose ps`).
2. Open http://localhost:8080
3. Find `example_hello` in the DAG list, unpause it (toggle on the left).
4. Click the play button > "Trigger DAG".
5. Watch the Grid view fill in green.
6. Click on a task square to see its logs.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="example_hello",
    start_date=datetime(2026, 5, 1),
    schedule=None,            # manual trigger only
    catchup=False,
    tags=["lab3", "example"],
)
def example_hello():

    @task
    def say_hello() -> str:
        msg = "Hello, EPF!"
        print(msg)
        return msg

    @task
    def say_goodbye(previous: str) -> str:
        msg = f"Got '{previous}'. Goodbye, EPF!"
        print(msg)
        return msg

    hello = say_hello()
    goodbye = say_goodbye(hello)
    # Explicit edge: same run order as passing `hello` into `say_goodbye`, but spelt
    # like traditional Airflow (BashOperator, PythonOperator, etc.).
    hello >> goodbye


example_hello()
