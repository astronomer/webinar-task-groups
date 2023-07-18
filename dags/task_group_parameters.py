"""
## DAG showing available task group parameters

This DAG shows task group parameters.
"""

from airflow.decorators import dag, task_group
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "TaskGroup", "parameters"],
)
def task_group_parameters():
    @task_group(
        group_id="task_group_1",
        default_args={"conn_id": "postgres_default"},
        tooltip="This task group is very important!",
        ui_color="#7352BA",  # Background color in graph view
        ui_fgcolor="#FFFFFF",  # Font color for in graph view
        prefix_group_id=True,
        # parent_group=None,
        # dag=None,
    )
    def tg1():
        t1 = EmptyOperator(task_id="t1")

    tg1()

    with TaskGroup(
        group_id="task_group_2",
        default_args={"conn_id": "postgres_default"},
        tooltip="This task group is also very important!",
        ui_color="#7352BA",  # Background color in graph view
        ui_fgcolor="#FFFFFF",  # Font color for in graph view
        prefix_group_id=True,
        # parent_group=None,
        # dag=None,
    ) as tg2:
        t1 = EmptyOperator(task_id="t1")

    with TaskGroup(
        group_id="task_group_2",
        add_suffix_on_collision=True,  # adds __1 to the task group id because the group_id already exists
    ) as tg2:
        t2 = EmptyOperator(task_id="t2")


task_group_parameters()
