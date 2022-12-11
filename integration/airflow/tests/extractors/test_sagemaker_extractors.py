# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import logging
import unittest
from unittest import mock, TestCase
import random
import pytz
from openlineage.airflow.extractors.sagemaker_extractors import (
    SageMakerProcessingExtractor,
    SageMakerTrainingExtractor,
    SageMakerTransformExtractor
)

from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator
)

from datetime import datetime
from airflow.models import TaskInstance, DAG
from airflow.utils.state import State
from airflow.utils import timezone
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION

log = logging.getLogger(__name__)


class TestSageMakerProcessingExtractor(TestCase):
    def setUp(self):
        log.debug("TestRedshiftDataExtractor.setup(): ")
        self.task = TestSageMakerProcessingExtractor._get_processing_task()
        self.ti = TestSageMakerProcessingExtractor._get_ti(task=self.task)
        self.extractor = SageMakerProcessingExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_extract_on_complete(self, mock_xcom_pull):
        self.extractor._get_s3_datasets = mock.MagicMock(return_value=([], []))

        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        self.extractor._get_s3_datasets.assert_called_once()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("openlineage.airflow.extractors.sagemaker_extractors.generate_s3_dataset")
    def test_missing_inputs_output(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor._get_s3_datasets = mock.MagicMock(return_value=([], []))

        self.extractor.extract_on_complete(self.ti)

        self.extractor._get_s3_datasets.assert_not_called()
        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_processing_task():
        dag = DAG(dag_id="TestSagemakerProcessingExtractor")
        task = SageMakerProcessingOperator(
            task_id="task_id",
            config={},
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


class TestSageMakerTransformExtractor(TestCase):
    def setUp(self):
        log.debug("TestRedshiftDataExtractor.setup(): ")
        self.task = TestSageMakerTransformExtractor._get_transform_task()
        self.ti = TestSageMakerTransformExtractor._get_ti(task=self.task)
        self.extractor = SageMakerTransformExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_extract_on_complete(self, mock_xcom_pull):
        self.extractor._get_model_data_urls = mock.MagicMock(return_value=([]))

        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        self.extractor._get_model_data_urls.assert_called_once()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("openlineage.airflow.extractors.sagemaker_extractors.generate_s3_dataset")
    def test_missing_inputs_output(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor._get_model_data_urls = mock.MagicMock(return_value=([]))

        self.extractor.extract_on_complete(self.ti)

        self.extractor._get_model_data_urls.assert_not_called()
        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_transform_task():
        dag = DAG(dag_id="TestSageMakerTransformExtractor")
        task = SageMakerTransformOperator(
            task_id="task_id",
            config={},
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


class TestSageMakerTrainingExtractor(TestCase):
    def setUp(self):
        log.debug("TestSageMakerTrainingExtractor.setup(): ")
        self.task = TestSageMakerTrainingExtractor._get_training_task()
        self.ti = TestSageMakerTrainingExtractor._get_ti(task=self.task)
        self.extractor = SageMakerTrainingExtractor(operator=self.task)

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    @mock.patch("openlineage.airflow.extractors.sagemaker_extractors.generate_s3_dataset")
    def test_extract_on_complete(self, mock_generate_s3_dataset, mock_xcom_pull):
        self.extractor.extract_on_complete(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)
        mock_generate_s3_dataset.assert_called()

    @mock.patch("airflow.models.TaskInstance.xcom_pull", return_value={})
    @mock.patch("openlineage.airflow.extractors.sagemaker_extractors.generate_s3_dataset")
    def test_generate_s3_dataset_missing_inputs_output(
            self, mock_generate_s3_dataset,
            mock_xcom_pull
    ):
        self.extractor.extract_on_complete(self.ti)

        mock_generate_s3_dataset.assert_not_called()

    @staticmethod
    def _get_training_task():
        dag = DAG(dag_id="TestSagemakerTrainingExtractor")
        task = SageMakerTrainingOperator(
            task_id="task_id",
            config={},
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
