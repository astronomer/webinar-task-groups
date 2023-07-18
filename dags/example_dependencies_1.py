"""
## Set a dependency between two tasks nested in task groups

This DAG shows how to set a dependency between two nested tasks, inner task
groups as a whole and the outer task groups. 
A different way to achieve the same pattern is shown in example_dependencies_2.py.
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
def example_dependencies_1():
    @task_group
    def top_level_task_group_1():
        @task_group
        def nested_task_group_1():
            t1 = EmptyOperator(task_id="t1")
            hello_one = EmptyOperator(task_id="hello")
            t2 = EmptyOperator(task_id="t2")
            t3 = EmptyOperator(task_id="t3")
            t1 >> hello_one >> t2

            return [t1, hello_one, t2, t3]  # return list of all tasks

        return nested_task_group_1()  # return inner task group object = list of tasks

    @task_group
    def top_level_task_group_2():
        @task_group
        def nested_task_group_2():
            t1 = EmptyOperator(task_id="t4")
            hello_two = EmptyOperator(task_id="hello")
            t2 = EmptyOperator(task_id="t5")
            t1 >> hello_two >> t2

            return [t1, hello_two, t2]  # return list of all tasks

        return nested_task_group_2()  # return inner task group object = list of tasks

    tg1_tasks = top_level_task_group_1()
    tg2_tasks = top_level_task_group_2()

    # setting the dependency between the two `hello` tasks
    tg1_tasks[1] >> tg2_tasks[1]

    # setting the dependency between the two nested task groups using the
    # `.task_group` attribute of individual tasks
    tg1_tasks[0].task_group >> tg2_tasks[0].task_group

    # setting the dependency between the two outer task groups using the
    # `.parent_task_group` attribute of the task group object
    tg1_tasks[0].task_group.parent_group >> tg2_tasks[0].task_group.parent_group


example_dependencies_1()
