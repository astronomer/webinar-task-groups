"""
## Simple example for passing information with the @task_group decorator using XCom

Showing how to pass information into and out of a task group using both 
the TaskFlow API and XCom directly.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "passing_information"],
)
def passing_information_2():
    @task
    def upstream():
        return "Hello there! :)"

    @task_group
    def tg1(my_message):
        @task
        def t1(my_string):
            return my_string + " I am T1!"

        @task
        def t2(**context):
            my_original_message = context["ti"].xcom_pull(task_ids="upstream")
            what_t1_says = context["ti"].xcom_pull(task_ids="tg1.t1")

            return (
                "The original message was: "
                + my_original_message
                + " T1 said: "
                + what_t1_says
                + " I am T2!"
            )

        t1(my_string=my_message) >> t2()

    @task
    def downstream(**context):
        what_t2_says = context["ti"].xcom_pull(task_ids="tg1.t2")
        print("T2 said: " + what_t2_says)

    tg1(upstream()) >> downstream()


passing_information_2()
