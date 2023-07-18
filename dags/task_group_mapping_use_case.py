"""
### Dynamically map over a task group to query an API and save the results to S3

This DAG showcases how to use dynamic mapping of a task group to query an API 
with different parameters and save the results to S3.
Needs an AWS connection set up with the ID `aws_conn`.
"""

from airflow.decorators import dag, task_group, task
from pendulum import datetime
import requests
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import json

MY_S3_KEY = "s3://mytxtbucket"
AWS_CONN_ID = "aws_conn"
MY_COUNTRY_1 = "US"
MY_COUNTRY_2 = "CH"


@dag(
    dag_id="task_group_mapping_and_pulling_usecase",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    tags=["@task_group", "use_case", "dynamic"],
)
def task_group_mapping_use_case():
    # the task group to fetch and save holidays from the input country
    @task_group(group_id="fetch_and_save_holidays")
    def fetch_and_save_holidays(country):
        # fetch holidays from a public API
        @task
        def extract_holidays(country):
            r = requests.get(
                f"https://date.nager.at/api/v3/PublicHolidays/2023/{country}"
            )
            return json.dumps(r.json())

        holidays_info = extract_holidays(country)

        # write the information into a file in an S3 bucket
        write_holidays_to_S3 = S3CreateObjectOperator(
            task_id="write_holidays_to_S3",
            aws_conn_id=AWS_CONN_ID,
            data=holidays_info,
            replace=True,
            s3_key=f"{MY_S3_KEY}/{country}.txt",
        )

        # set dependencies
        holidays_info >> write_holidays_to_S3

    # a downstream task to print out the result of a specific task in a mapped task group
    @task
    def print_holidays_in_country_2(**context):
        pulled_xcom = context["ti"].xcom_pull(
            task_ids=["fetch_and_save_holidays.extract_holidays"],
            map_indexes=1,  # specify the map indexes to pull from (2.5 feature)
            key="return_value",
        )

        print(pulled_xcom)

    # creating two mapped task groups for two countries (2.5 feature)
    tg_object = fetch_and_save_holidays.expand(country=[MY_COUNTRY_1, MY_COUNTRY_2])

    # setting dependencies
    tg_object >> print_holidays_in_country_2()


task_group_mapping_use_case()