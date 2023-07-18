# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from minio import Minio
import logging

# -------------------- #
# Local module imports #
# -------------------- #

MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"


def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client


task_log = logging.getLogger("airflow.task")

# --------------- #
# TaskGroup class #
# --------------- #


class CreateBucket(TaskGroup):
    """A task group to create a bucket if it does not already exist."""

    def __init__(self, group_id, bucket_name=None, **kwargs):
        """Instantiate a CreateBucketTaskGroup."""
        super().__init__(
            group_id=group_id, ui_color="#00A7FB", **kwargs
        )

        # --------------------- #
        # List Buckets in MinIO #
        # --------------------- #

        @task(task_group=self)
        def list_buckets_minio():
            """Returns the list of all bucket names in a MinIO instance."""

            # use a utility function to get the MinIO client
            client = get_minio_client()
            buckets = client.list_buckets()
            existing_bucket_names = [bucket.name for bucket in buckets]
            task_log.info(f"MinIO contains: {existing_bucket_names}")

            return existing_bucket_names

        # -------------------------------------- #
        # Decide if a bucket needs to be created #
        # -------------------------------------- #

        @task.branch(task_group=self)
        def decide_whether_to_create_bucket(buckets):
            """Returns a task_id depending on whether the bucket name provided
            to the class is in the list of buckets provided as an argument."""

            if bucket_name in buckets:
                return f"{group_id}.bucket_already_exists"
            else:
                return f"{group_id}.create_bucket"

        # ------------- #
        # Create Bucket #
        # ------------- #

        @task(task_group=self)
        def create_bucket():
            """Creates a bucket in MinIO."""

            client = get_minio_client()
            client.make_bucket(bucket_name)

        # ----------------------------- #
        # Empty Operators for structure #
        # ----------------------------- #

        bucket_already_exists = EmptyOperator(
            task_id="bucket_already_exists", task_group=self
        )

        bucket_exists = EmptyOperator(
            task_id="bucket_exists",
            trigger_rule="none_failed_min_one_success",
            task_group=self,
        )

        # set dependencies within task group
        branch_task = decide_whether_to_create_bucket(list_buckets_minio())
        branch_options = [create_bucket(), bucket_already_exists]
        branch_task >> branch_options >> bucket_exists
