"""
## Simple example using default_args vs direct passing of kwargs

Showing how to pass default args and kwargs with task groups.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "TaskGroup", "default_args"],
)
def use_default_args():
    @task_group(
        group_id="task_group_1",
        default_args={
            "my_default_arg": "Hello!",
            "bash_command": "echo hi",
        },  # this `bash_command` is picked up by the BashOperator
    )
    def tg1(
        direct_arg, bash_command
    ):  # this `bash_command` is NOT picked up by the BashOperator
        BashOperator(task_id="t1")
        BashOperator(task_id="t2", bash_command="echo 0")
        BashOperator(task_id="t3", bash_command=bash_command)

        @task
        def print_a_default_arg(my_default_arg):
            print(my_default_arg)

        print_a_default_arg()

        @task
        def print_direct_arg_1(arg):
            print(arg)

        @task
        def print_direct_arg_2(arg):
            print(arg)

        print_direct_arg_1(direct_arg)
        print_direct_arg_2(bash_command)

    tg1_object = tg1(direct_arg="Hola!", bash_command="echo Hallo!")

    with TaskGroup(
        group_id="task_group_2",
        default_args={"my_default_arg": "Hello!", "bash_command": "echo hi"},
    ) as tg2:
        BashOperator(task_id="t1")
        BashOperator(task_id="t2", bash_command="echo 0")

        @task
        def print_a_default_arg(my_default_arg):
            print(my_default_arg)

        print_a_default_arg()

    tg1_object >> tg2


use_default_args()
