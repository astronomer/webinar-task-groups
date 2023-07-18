import datetime

from airflow.decorators import dag, task, task_group


def custom_task_group(python_callable=None, **kwargs):
    def new_callable_func():
        python_callable()
        @task
        def do_thing_d():
            print("d")

        do_thing_d()

    return task_group(python_callable=new_callable_func, **kwargs)


@dag(start_date=datetime.datetime(2023, 1, 1), schedule=None)
def override_task_group_decorator():
    @custom_task_group(group_id="my_task_group")
    def my_tasks():
        @task
        def do_thing_a():
            print("a")

        @task
        def do_thing_b():
            print("b")

        @task
        def do_thing_c():
            print("c")

        do_thing_a()
        do_thing_b()
        do_thing_c()

    my_tasks()


override_task_group_decorator()
