"""
## Set a dependency between two tasks nested in task groups

This DAG shows how to set a dependency between two nested tasks, inner task
groups as a whole and the outer task groups.
A different way to achieve the same pattern is shown in example_dependencies_1.py.
For the same pattern with TaskGroup see example_dependencies_3.py.
"""

from airflow.decorators import dag, task_group
from pendulum import datetime
from airflow.operators.empty import EmptyOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "dependencies"],
)
def example_dependencies_2():
    # declare variables
    hello_g1: EmptyOperator
    nested_task_group_1_object: task_group

    @task_group
    def top_level_task_group_1():
        @task_group
        def nested_task_group_1():
            # set variable as nonlocal to be accessible outside the task group function
            nonlocal hello_g1
            t1 = EmptyOperator(task_id="t1")
            hello_g1 = EmptyOperator(task_id="hello")
            t2 = EmptyOperator(task_id="t2")
            t3 = EmptyOperator(task_id="t3")
            t1 >> hello_g1 >> t2

        # set task group object as nonlocal to be accessible outside the task group function
        nonlocal nested_task_group_1_object
        nested_task_group_1_object = nested_task_group_1()

    @task_group
    def top_level_task_group_2():
        @task_group
        def nested_task_group_2():
            t1 = EmptyOperator(task_id="t4")
            hello_g2 = EmptyOperator(task_id="hello")
            t2 = EmptyOperator(task_id="t5")
            t1 >> hello_g2 >> t2

            # set the dependency between the hello tasks
            hello_g1 >> hello_g2

        # set the dependency between the nested task groups
        nested_task_group_1_object >> nested_task_group_2()

    # set the dependency between the top level task groups as a whole in the usual way
    top_level_task_group_1() >> top_level_task_group_2()


example_dependencies_2()
