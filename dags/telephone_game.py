"""
## Play the telephone game with Airflow! (and learn about task groups)

This DAG shows how to use task groups, pass information between them and 
nest them.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
import random
import string


SECRET_WORD = "Avery likes to cuddle"
NUM_LETTERS_TO_REPLACE_OUTER_TG = 2
NUM_LETTERS_TO_REPLACE_INNER_TG = 3


@task
def misunderstand_task(num_letters_to_replace, word):
    for _ in range(num_letters_to_replace):
        random_index = random.randint(0, len(word) - 1)
        random_letter = random.choice(string.ascii_lowercase)
        word = word[:random_index] + random_letter + word[random_index + 1 :]
    return word


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["webinar", "@task_group", "nesting", "passing_information"],
)
def telephone_game_1():
    @task
    def upstream():
        secret_word = SECRET_WORD
        return secret_word

    @task_group(
        tooltip="Playing the telephone game!",
        default_args={"num_letters_to_replace": NUM_LETTERS_TO_REPLACE_OUTER_TG},
    )
    def tg1(my_word):
        new_word_1 = misunderstand_task.override(task_id="t1")(word=my_word)
        new_word_2 = misunderstand_task.override(task_id="t2")(word=new_word_1)

        @task_group(
            group_id="tg2",
            default_args={"num_letters_to_replace": NUM_LETTERS_TO_REPLACE_INNER_TG},
        )
        def tg2(new_word_2):
            new_word_3 = misunderstand_task.override(task_id="t4")(word=new_word_2)
            new_word_4 = misunderstand_task.override(task_id="t5")(word=new_word_3)
            return new_word_4

        new_word_4 = tg2(new_word_2)

        return new_word_4

    @task
    def downstream(new_word):
        return "The secret word was: " + new_word

    upstream_task_output = upstream()
    tg1_output = tg1(upstream_task_output)
    downstream(tg1_output)


telephone_game_1()
