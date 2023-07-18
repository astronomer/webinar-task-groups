"""
## A TaskGroup

Simplest TaskGroup.
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from pendulum import datetime


with DAG(
    dag_id="simple_task_group_traditional",
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup"],
):
    with TaskGroup(group_id="tg1") as tg1:
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")

        t1 >> t2

    tg1
