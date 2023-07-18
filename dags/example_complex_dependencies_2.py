"""
## Set complex dependencies between tasks and task groups

This DAG shows how to set different dependencies between tasks and task groups.
For the same pattern with @task_group see example_complex_dependencies_1.py.
"""

from airflow.decorators import dag
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup", "dependencies"],
)
def example_complex_dependencies_2():
    top_level_task_0 = EmptyOperator(task_id="top_level_task_0")
    top_level_task_1 = EmptyOperator(task_id="top_level_task_1")
    top_level_task_2 = EmptyOperator(task_id="top_level_task_2")
    top_level_task_3 = EmptyOperator(task_id="top_level_task_3")

    with TaskGroup(group_id="top_level_task_group_1") as tg1:
        hello_task_1 = EmptyOperator(task_id="HELLO")
        nested_task_2_level_1 = EmptyOperator(task_id="nested_task_2_level_1")
        independent_task_in_tg = EmptyOperator(task_id="independent_task_in_tg")

        top_level_task_1 >> hello_task_1 >> nested_task_2_level_1
        top_level_task_2 >> nested_task_2_level_1
        nested_task_2_level_1 >> top_level_task_3

    with TaskGroup(group_id="top_level_task_group_2") as tg2:
        nested_task_3_level_1 = EmptyOperator(task_id="nested_task_3_level_1")

        with TaskGroup(group_id="nested_task_group_1") as tg3:
            hello_task_2 = EmptyOperator(task_id="HELLO")
            nested_task_4_level_1 = EmptyOperator(task_id="nested_task_4_level_1")

            top_level_task_1 >> hello_task_2 >> nested_task_4_level_1
            top_level_task_2 >> nested_task_4_level_1
            nested_task_4_level_1 >> top_level_task_3

        tg3 >> nested_task_3_level_1

    top_level_task_0 >> tg1 >> tg2

    hello_task_1 >> hello_task_2


example_complex_dependencies_2()
