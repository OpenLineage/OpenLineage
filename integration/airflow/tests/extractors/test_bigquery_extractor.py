# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import random
import unittest
import uuid
from datetime import datetime

import mock
from pkg_resources import parse_version
import pytz
from airflow.utils import timezone
from airflow.models import TaskInstance, DAG
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION

from openlineage.airflow.extractors.bigquery_extractor import BigQueryExtractor
from openlineage.airflow.utils import try_import_from_string
from openlineage.client.facet import OutputStatisticsOutputDatasetFacet, ExternalQueryRunFacet
from openlineage.common.provider.bigquery import BigQueryJobRunFacet, \
    BigQueryStatisticsDatasetFacet, BigQueryErrorRunFacet
from openlineage.common.utils import get_from_nullable_chain

log = logging.getLogger(__name__)


BigQueryExecuteQueryOperator = try_import_from_string(
    "airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator"
)


class TestBigQueryExtractorE2E(unittest.TestCase):
    def setUp(self):
        log.debug("TestBigQueryExtractorE2E.setup(): ")

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_extract(self, mock_hook):
        log.info("test_extractor")

        job_details = self.read_file_json(
            "tests/extractors/job_details.json")
        table_details = self.read_dataset_json(
            "tests/extractors/table_details.json")
        out_details = self.read_dataset_json(
            "tests/extractors/out_table_details.json")

        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .run_query.return_value = bq_job_id

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        mock_client = mock.MagicMock()

        project_id = "test"
        mock_hook.return_value.project_id = project_id
        mock_hook.return_value.location = "US"
        mock_hook.return_value.get_client.return_value = mock_client

        mock_client.get_job.return_value._properties = job_details

        mock_client.get_table.side_effect = [table_details, out_details]

        # To make sure hasattr "sees" close and calls it
        mock_client.close.return_value

        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        dag.create_dagrun(
            run_id=str(uuid.uuid4()),
            state=State.QUEUED,
            execution_date=execution_date
        )

        task = BigQueryExecuteQueryOperator(
            sql='select first_name, last_name from dataset.customers;',
            task_id="task_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            do_xcom_push=False
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=execution_date
        )

        bq_extractor = BigQueryExtractor(task)
        task_meta_extract = bq_extractor.extract()
        assert task_meta_extract is None

        task_instance.run()

        task_meta = bq_extractor.extract_on_complete(task_instance)

        mock_hook.return_value.get_client.assert_called_with(
            project_id=project_id,
            location="US"
        )
        mock_client.get_job.assert_called_once_with(job_id=bq_job_id)

        assert task_meta.inputs is not None
        assert len(task_meta.inputs) == 1
        assert task_meta.inputs[0].name == \
            'bigquery-public-data.usa_names.usa_1910_2013'

        assert task_meta.inputs[0].facets['schema'].fields is not None
        assert task_meta.inputs[0].facets['dataSource'].name == 'bigquery'
        assert task_meta.inputs[0].facets['dataSource'].uri == 'bigquery'
        assert len(task_meta.inputs[0].facets['schema'].fields) == 5
        assert task_meta.outputs is not None
        assert len(task_meta.outputs) == 1
        assert task_meta.outputs[0].facets['schema'].fields is not None
        assert len(task_meta.outputs[0].facets['schema'].fields) == 2
        assert task_meta.outputs[0].name == \
            'bq-airflow-openlineage.new_dataset.output_table'

        assert BigQueryStatisticsDatasetFacet(
            rowCount=20,
            size=321
        ) == task_meta.outputs[0].facets['stats']

        assert OutputStatisticsOutputDatasetFacet(
            rowCount=20,
            size=321
        ) == task_meta.outputs[0].outputFacets['outputStatistics']

        assert len(task_meta.run_facets) == 2
        assert BigQueryJobRunFacet(
            cached=False,
            billedBytes=111149056,
            properties=json.dumps(job_details)
        ) == task_meta.run_facets['bigQuery_job']

        assert ExternalQueryRunFacet(
            externalQueryId=bq_job_id,
            source="bigquery"
        ) == task_meta.run_facets['externalQuery']

        mock_client.close.assert_called()

    def read_dataset_json(self, file):
        client_mock = self.Client_mock()
        client_mock._properties = self.read_file_json(file)
        return client_mock

    class Client_mock:
        _properties = None

    def read_file_json(self, file):
        f = open(
            file=file,
            mode="r"
        )
        details = json.loads(f.read())
        f.close()
        return details

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_extract_cached(self, mock_hook):
        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .run_query.return_value = bq_job_id

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        job_details = self.read_file_json(
            "tests/extractors/cached_job_details.json"
        )

        mock_client = mock.MagicMock()

        project_id = "test"
        mock_hook.return_value.project_id = project_id
        mock_hook.return_value.location = "US"
        mock_hook.return_value.get_client.return_value = mock_client

        mock_client.get_job.return_value._properties = job_details
        # To make sure hasattr "sees" close and calls it
        mock_client.close.return_value
        mock.seal(mock_client)

        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        dag.create_dagrun(
            run_id=str(uuid.uuid4()),
            state=State.QUEUED,
            execution_date=execution_date
        )

        task = BigQueryExecuteQueryOperator(
            sql='select first_name, last_name from dataset.customers;',
            task_id="task_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            do_xcom_push=False
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=execution_date
        )

        bq_extractor = BigQueryExtractor(task)
        tasks_meta_extract = bq_extractor.extract()
        assert tasks_meta_extract is None

        task_instance.run()

        task_meta = bq_extractor.extract_on_complete(task_instance)
        assert task_meta.inputs is not None
        assert task_meta.outputs is not None

        assert len(task_meta.run_facets) == 2
        print(task_meta.run_facets.keys())
        assert task_meta.run_facets['bigQuery_job'] \
               == BigQueryJobRunFacet(cached=True)

        assert ExternalQueryRunFacet(
            externalQueryId=bq_job_id,
            source="bigquery"
        ) == task_meta.run_facets['externalQuery']

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_extract_error(self, mock_hook):
        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .run_query.return_value = bq_job_id

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        mock_client = mock.MagicMock()

        project_id = "test"
        mock_hook.return_value.project_id = project_id
        mock_hook.return_value.location = "US"
        mock_hook.return_value.get_client.return_value = mock_client

        mock_client.get_job.side_effects = [Exception("bq error")]

        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        dag.create_dagrun(
            run_id=str(uuid.uuid4()),
            state=State.QUEUED,
            execution_date=execution_date
        )

        task = BigQueryExecuteQueryOperator(
            sql='select first_name, last_name from dataset.customers;',
            task_id="task_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            do_xcom_push=False
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=execution_date
        )

        bq_extractor = BigQueryExtractor(task)

        tasks_meta_extract = bq_extractor.extract()
        assert tasks_meta_extract is None

        task_instance.run()

        task_meta = bq_extractor.extract_on_complete(task_instance)

        assert task_meta.run_facets['bigQuery_error'] == BigQueryErrorRunFacet(
            clientError=mock.ANY
        )
        mock_client.get_job.assert_called_once_with(job_id=bq_job_id)

        assert task_meta.inputs is not None
        assert len(task_meta.inputs) == 0
        assert task_meta.outputs is not None
        assert len(task_meta.outputs) == 0


class TestBigQueryExtractor(unittest.TestCase):
    def setUp(self):
        log.debug("TestBigQueryExtractor.setup(): ")
        self.task = TestBigQueryExtractor._get_bigquery_task()
        self.ti = TestBigQueryExtractor._get_ti(task=self.task)
        self.bq_extractor = BigQueryExtractor(operator=self.task)

    def test_extract(self):
        log.info("test_extractor")
        tasks_meta_extract = BigQueryExtractor(self.task).extract()
        assert tasks_meta_extract is None

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_get_xcom_bigquery_job_id(self, mock_xcom_pull):
        self.bq_extractor._get_xcom_bigquery_job_id(self.ti)

        mock_xcom_pull.assert_called_once_with(
            task_ids=self.ti.task_id, key='job_id')

    def test_nullable_chain_fails(self):
        x = {"first": {"second": {}}}
        assert get_from_nullable_chain(x, ['first', 'second', 'third']) is None

    def test_nullable_chain_works(self):
        x = {"first": {"second": {"third": 42}}}
        assert get_from_nullable_chain(x, ['first', 'second', 'third']) == 42

        x = {"first": {"second": {"third": 42, "fourth": {"empty": 56}}}}
        assert get_from_nullable_chain(x, ['first', 'second', 'third']) == 42

    @staticmethod
    def _get_ti(task):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs['run_id'] = 'test_run_id'  # change in 2.2.0
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.RUNNING,
            **kwargs)
        task_instance.job_id = random.randrange(10000)

        return task_instance

    @staticmethod
    def _get_async_job(properties):
        # BigQuery Job
        class AsyncJob:
            _properties = None

            def __init__(self, _properties):
                self._properties = _properties

        return AsyncJob(_properties=properties)

    @staticmethod
    def _get_bigquery_task():
        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        task = BigQueryExecuteQueryOperator(
            sql='select first_name, last_name from dataset.customers;',
            task_id="task_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0)
        )

        return task


if __name__ == '__main__':
    unittest.main()
