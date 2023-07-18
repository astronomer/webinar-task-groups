"""
## Use a custom task group in a DAG

This DAG shows usage of a custom task group in a DAG with one varied parameter.
"""

from airflow.decorators import dag
from pendulum import datetime
from include.custom_task_groups.create_bucket_task_group import CreateBucket


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
    tags=["webinar", "custom_task_group"],
)
def custom_task_group_use_case():
    tg1 = CreateBucket(group_id="t1", bucket_name="bucket1")
    tg2 = CreateBucket(group_id="t2", bucket_name="bucket2")
    tg3 = CreateBucket(group_id="t3", bucket_name="bucket3")
    tg4 = CreateBucket(group_id="t4", bucket_name="bucket4")

    tg1 >> [tg2, tg3] >> tg4


custom_task_group_use_case()
