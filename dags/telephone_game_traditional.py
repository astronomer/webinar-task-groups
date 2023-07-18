"""
## Play the telephone game with Airflow! (and learn about task groups)

This DAG shows how to use task groups, pass information between them and 
nest them.
"""

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from pendulum import datetime
import random
import string


SECRET_WORD = "Avery likes to cuddle"
NUM_LETTERS_TO_REPLACE_OUTER_TG = 2
NUM_LETTERS_TO_REPLACE_INNER_TG = 3


def upstream_func(secret_word):
    return secret_word


def misunderstand_func(word, num_letters_to_replace):
    for _ in range(num_letters_to_replace):
        random_index = random.randint(0, len(word) - 1)
        random_letter = random.choice(string.ascii_lowercase)
        word = word[:random_index] + random_letter + word[random_index + 1 :]
    return word


def downstream_func(new_word):
    return "The secret word was: " + new_word


with DAG(
    dag_id="telephone_game_2",
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["TaskGroup", "nesting", "passing_information"],
):
    upstream = PythonOperator(
        task_id="upstream",
        python_callable=upstream_func,
        op_kwargs={"secret_word": SECRET_WORD},
    )

    with TaskGroup(
        group_id="tg1",
    ) as tg1:
        t1 = PythonOperator(
            task_id="t1",
            python_callable=misunderstand_func,
            op_kwargs={
                "word": upstream.output,
                "num_letters_to_replace": NUM_LETTERS_TO_REPLACE_OUTER_TG,
            },
        )

        t2 = PythonOperator(
            task_id="t2",
            python_callable=misunderstand_func,
            op_kwargs={
                "word": t1.output,
                "num_letters_to_replace": NUM_LETTERS_TO_REPLACE_OUTER_TG,
            },
        )

        with TaskGroup(
            group_id="tg2",
        ) as tg2:
            t4 = PythonOperator(
                task_id="t4",
                python_callable=misunderstand_func,
                op_kwargs={
                    "word": t2.output,
                    "num_letters_to_replace": NUM_LETTERS_TO_REPLACE_INNER_TG,
                },
            )

            t5 = PythonOperator(
                task_id="t5",
                python_callable=misunderstand_func,
                op_kwargs={
                    "word": t4.output,
                    "num_letters_to_replace": NUM_LETTERS_TO_REPLACE_INNER_TG,
                },
            )

    downstream = PythonOperator(
        task_id="downstream",
        python_callable=downstream_func,
        op_kwargs={"new_word": t5.output},
    )

    upstream >> tg1 >> downstream
