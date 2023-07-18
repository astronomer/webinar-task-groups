"""
## Draw a webinar agenda using task groups with custom colors

This DAG shows the Agenda of the task groups webinar. Features shown are
custom task group colors, nesting of task groups and dependency setting.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["UI", "nesting", "@task_group", "dependencies"],
)
def agenda_dag():
    @task(task_id="Airflow_basics")
    def t0():
        return 1

    @task_group(
        group_id="Task_groups",
        ui_color="#7352BA",
    )
    def tg1():
        @task_group(
            group_id="Basics",
            ui_color="#00A7FB",
        )
        def tg2():
            @task(task_id="what_and_why")
            def t0():
                return 1

            @task(task_id="TaskGroup")
            def t1():
                return 1

            @task(task_id="task_group_decorator")
            def t2():
                return 1

            @task(task_id="Parameters")
            def t3():
                return 1

            @task(task_id="group_id.task_id")
            def t4():
                return 1

            t0() >> [t1(), t2()] >> t3() >> t4()

        @task_group(
            group_id="Using_task_groups",
            ui_color="#4FCEBA",
        )
        def tg3():
            @task(task_id="Passing_information")
            def t5():
                return 1

            @task_group(ui_color="#0078ce")
            def nesting():
                @task(task_id="Nesting")
                def t6():
                    return 1

                return t6()

            @task(task_id="Dependencies")
            def t7():
                return 1

            t5() >> nesting() >> t7()

        @task_group(
            group_id="Demo_and_advanced_use_cases",
            ui_color="#FF865E",
        )
        def tg4():
            @task(task_id="Task_group_example")
            def t8():
                return 1

            @task(task_id="Dynamic_tg_mapping")
            def t9():
                return 1

            @task(task_id="Custom_task_group_classes")
            def t10():
                return 1

            @task(task_id="Existing_task_group_modules")
            def t7():
                return 1

            t8() >> t9() >> [t7(), t9()]

        tg2() >> [tg3(), tg4()]

    @task(task_id="Questions")
    def tn():
        return 1

    t0() >> tg1() >> tn()


agenda_dag()
