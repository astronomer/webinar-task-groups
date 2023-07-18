"""
## Example structure for task group nesting

Showing how to nest task groups.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "UI", "nesting"],
)
def nesting_model_training():

    @task 
    def upstream():
        return 1

    @task_group
    def model_training():

        @task
        def start_cluster():
            return 1

        @task_group
        def model_1():
            @task
            def train_model():
                return 1

            @task
            def validate_model():
                return 1
            
            train_model() >> validate_model()
            
        @task_group
        def model_2():
            @task
            def train_model():
                return 1

            @task
            def validate_model():
                return 1
            
            train_model() >> validate_model()

        @task 
        def compare_models():
            return 1

        start_cluster() >> [model_1(), model_2()] >> compare_models()

    @task 
    def downstream():
        return 1
    
    upstream() >> model_training() >> downstream()


nesting_model_training()
