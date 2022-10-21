from unittest import TestCase
import logging
import unittest
from unittest import mock, TestCase
import random
import pytz
from openlineage.airflow.extractors.s3_extractor import S3CopyObjectExtractor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from openlineage.airflow.extractors.base import TaskMetadata
from datetime import datetime
from airflow.models import TaskInstance, DAG
from airflow.utils.state import State
from airflow.utils import timezone
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION
from openlineage.client.run import Dataset

log = logging.getLogger(__name__)


class TestS3CopyObjectExtractor(TestCase):
    def setUp(self):
        log.debug("TestS3CopyObjectExtractor.setup(): ")
        self.task = TestS3CopyObjectExtractor._get_copy_task()
        self.ti = TestS3CopyObjectExtractor._get_ti(task=self.task)
        self.extractor = S3CopyObjectExtractor(operator=self.task)

    def test_extract(self):
        expected_return_value = TaskMetadata(
            name="TestS3CopyObjectExtractor.task_id",
            inputs=[
                Dataset(
                    namespace="s3://source-bucket",
                    name="s3://source-bucket/path/to/source_file.csv",
                    facets={}
                )
            ],
            outputs=[
                Dataset(
                    namespace="s3://destination-bucket",
                    name="s3://destination-bucket/path/to/destination_file.csv",
                    facets={}
                )
            ],
        )

        return_value = self.extractor.extract()

        self.assertEqual(return_value, expected_return_value)

    @staticmethod
    def _get_copy_task():
        dag = DAG(dag_id="TestS3CopyObjectExtractor")
        task = S3CopyObjectOperator(
            task_id="task_id",
            source_bucket_name="source-bucket",
            source_bucket_key="path/to/source_file.csv",
            dest_bucket_name="destination-bucket",
            dest_bucket_key="path/to/destination_file.csv",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )

        return task

    @staticmethod
    def _get_ti(task):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs['run_id'] = 'test_run_id'  # change in 2.2.0
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.SUCCESS,
            **kwargs)
        task_instance.job_id = random.randrange(10000)

        return task_instance

    if __name__ == '__main__':
        unittest.main()
