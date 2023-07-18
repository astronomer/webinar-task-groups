"""
## Example structure for task group nesting with both types of syntax

Showing how to nest task groups.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "TaskGroup", "UI", "nesting"],
)
def nesting_simple():
    # Nesting with decorator syntax

    @task_group
    def outer_task_group():
        @task_group
        def inner_task_group():
            @task
            def my_task():
                return 1

            my_task()

        inner_task_group()

    outer_task_group()

    # Nesting with context manager (traditional) syntax

    with TaskGroup(group_id="outer_task_group_2") as tg1:
        with TaskGroup(group_id="inner_task_group_2") as tg2:
            t1 = EmptyOperator(task_id="my_task_2")


nesting_simple()
