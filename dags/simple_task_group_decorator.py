"""
## A TaskGroup

Simplest TaskGroup.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group"],
)
def simple_task_group_decorator():
    @task_group
    def tg1():
        @task
        def t1():
            return 1

        @task
        def t2():
            return 1

        t1() >> t2()

    tg1()


simple_task_group_decorator()
