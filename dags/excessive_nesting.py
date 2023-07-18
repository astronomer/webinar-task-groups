"""
## Overly nested task groups DAG

Showing that there seems to be no limit for nesting.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "UI", "nesting"],
)
def excessive_nesting():
    @task_group
    def tg1():
        @task_group
        def tg2():
            @task_group
            def tg3():
                @task_group
                def tg4():
                    @task_group
                    def tg5():
                        @task_group
                        def tg6():
                            @task_group
                            def tg7():
                                @task_group
                                def tg8():
                                    @task
                                    def t1():
                                        return 1

                                    t1()

                                tg8()

                            tg7()

                        tg6()

                    tg5()

                tg4()

            tg3()

        tg2()

    tg1()


excessive_nesting()
