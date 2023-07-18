"""
## Set a dependency between two tasks nested in task groups

This DAG shows how to set a dependency between two nested tasks, inner task
groups as a whole and the outer task groups. 
For the same pattern with @task_group see example_dependencies_1.py and
example_dependencies_2.py.
"""

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup", "dependencies"],
)
def example_dependencies_3():
    with TaskGroup(group_id="top_level_task_group_1") as tg1:
        with TaskGroup(group_id="nested_task_group_1") as tg2:
            t1 = EmptyOperator(task_id="t1")
            hello_one = EmptyOperator(task_id="hello")
            t2 = EmptyOperator(task_id="t2")
            t3 = EmptyOperator(task_id="t3")
            t1 >> hello_one >> t2

    with TaskGroup(group_id="top_level_task_group_2") as tg3:
        with TaskGroup(group_id="nested_task_group_2") as tg4:
            t4 = EmptyOperator(task_id="t4")
            hello_two = EmptyOperator(task_id="hello")
            t5 = EmptyOperator(task_id="t5")

            t4 >> hello_two >> t5

    tg1 >> tg3
    tg2 >> tg4
    hello_one >> hello_two


example_dependencies_3()
