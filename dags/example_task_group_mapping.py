"""
## Dynamically map a task group over a list of numbers of unknown length.

This is an example of dynamic task group mapping.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
import random


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["webinar", "@task_group", "dynamic"],
)
def example_task_group_mapping():
    @task
    def get_list_of_numbers():
        length = random.randint(4, 8)
        random_list = [random.randint(0, 100) for _ in range(length)]
        print("Returning a list of length ", len(random_list))
        return random_list

    # creating a task group using the decorator with the dynamic input my_num
    @task_group(group_id="group1")
    def tg1(vegetables, my_num):
        @task
        def add_2(num):
            return num + 2

        @task
        def multiply_by_100(num):
            multiplied = num * 100
            print(f"We bought {multiplied} {vegetables}!")
            return multiplied

        multiply_by_100(add_2(my_num))

    # creating 6 mapped task group instances of the task group group1
    tg1.partial(vegetables="tomatoes!").expand(my_num=get_list_of_numbers())


example_task_group_mapping()
