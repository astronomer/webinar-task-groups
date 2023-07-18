"""
## Simple example for passing information with the @task_group decorator

Showing how to pass information  into and out of a task group with the 
TaskFlow API.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "passing_information"],
)
def passing_information_1():
    @task
    def upstream():
        return "Hello there! :)"

    @task_group
    def tg1(my_message):
        @task
        def t1(my_string):
            return my_string + " I am T1!"

        return t1(my_string=my_message)

    @task
    def downstream(message):
        print("T1 said: " + message)

    downstream(tg1(upstream()))


passing_information_1()
