# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import random
import unittest
from datetime import datetime

import mock
import pytz
from airflow.utils import timezone
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import TaskInstance, DAG
from airflow.utils.state import State

from marquez_airflow.extractors.bigquery_extractor import BigQueryExtractor, \
    BigQueryStaticticsRunFacet, \
    get_from_nullable_chain, \
    BigQueryErrorRunFacet, BigQueryStatisticsDatasetFacet

log = logging.getLogger(__name__)


class TestBigQueryExtractorE2E(unittest.TestCase):
    def setUp(self):
        log.debug("TestBigQueryExtractorE2E.setup(): ")

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    @mock.patch('google.cloud.bigquery.Client')
    def test_extract(self, mock_client, mock_hook):
        log.info("test_extractor")

        job_details = self.read_file_json(
            "tests/extractors/job_details.json")
        table_details = self.read_dataset_json(
            "tests/extractors/table_details.json")
        out_details = self.read_dataset_json(
            "tests/extractors/out_table_details.json")

        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        mock_client.return_value \
            .get_job.return_value \
            ._properties = job_details

        mock_client.return_value \
            .get_table.side_effect = [table_details, out_details]

        # To make sure hasattr "sees" close and calls it
        mock_client.return_value.close.return_value

        mock.seal(mock_hook)
        mock.seal(mock_client)

        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        task = BigQueryOperator(
            sql='select first_name, last_name from customers;',
            task_id="task_id",
            project_id="project_id",
            dag_id="dag_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0)
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc))

        bq_extractor = BigQueryExtractor(task)
        steps_meta_extract = bq_extractor.extract()
        assert steps_meta_extract is None

        task_instance.run()

        step_meta = bq_extractor.extract_on_complete(task_instance)
        mock_client.return_value \
            .get_job.assert_called_once_with(job_id=bq_job_id)

        assert step_meta.inputs is not None
        assert len(step_meta.inputs) == 1
        assert step_meta.inputs[0].name == \
            'bigquery-public-data.usa_names.usa_1910_2013'

        assert step_meta.inputs[0].fields is not None
        assert step_meta.inputs[0].source.connection_url == \
               'bigquery:bigquery-public-data.usa_names.usa_1910_2013'
        assert len(step_meta.inputs[0].fields) == 5
        assert step_meta.outputs is not None
        assert len(step_meta.outputs) == 1
        assert step_meta.outputs[0].fields is not None
        assert len(step_meta.outputs[0].fields) == 2
        assert step_meta.outputs[0].name == \
            'bq-airflow-marquez.new_dataset.output_table'

        assert BigQueryStatisticsDatasetFacet(
            rowCount=20,
            size=321
        ) == step_meta.outputs[0].custom_facets['stats']

        assert len(step_meta.run_facets) == 1
        assert BigQueryStaticticsRunFacet(
            cached=False,
            billedBytes=111149056,
            properties=json.dumps(job_details)
        ) == step_meta.run_facets['bigQuery_statistics']

        mock_client.return_value.close.assert_called()

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

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    @mock.patch('google.cloud.bigquery.Client')
    def test_extract_cached(self, mock_client, mock_hook):
        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        job_details = self.read_file_json(
            "tests/extractors/cached_job_details.json"
        )

        mock_client.return_value.get_job.return_value._properties = job_details
        # To make sure hasattr "sees" close and calls it
        mock_client.return_value.close.return_value

        mock.seal(mock_hook)
        mock.seal(mock_client)

        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        task = BigQueryOperator(
            sql='select first_name, last_name from customers;',
            task_id="task_id",
            project_id="project_id",
            dag_id="dag_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0)
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc))

        bq_extractor = BigQueryExtractor(task)
        steps_meta_extract = bq_extractor.extract()
        assert steps_meta_extract is None

        task_instance.run()

        step_meta = bq_extractor.extract_on_complete(task_instance)
        assert step_meta.inputs is not None
        assert step_meta.outputs is not None

        assert len(step_meta.run_facets) == 1
        assert step_meta.run_facets['bigQuery_statistics'] \
               == BigQueryStaticticsRunFacet(cached=True)

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    @mock.patch('google.cloud.bigquery.Client')
    def test_extract_error(self, mock_client, mock_hook):
        bq_job_id = "foo.bq.job_id"

        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query.return_value = bq_job_id

        mock_client.return_value \
            .get_job.side_effects = [Exception("bq error")]

        # To make sure hasattr "sees" close and calls it
        mock_client.return_value.close.return_value

        mock.seal(mock_hook)
        mock.seal(mock_client)

        dag = DAG(dag_id='TestBigQueryExtractorE2E')
        task = BigQueryOperator(
            sql='select first_name, last_name from customers;',
            task_id="task_id",
            project_id="project_id",
            dag_id="dag_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0)
        )

        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc))

        bq_extractor = BigQueryExtractor(task)

        steps_meta_extract = bq_extractor.extract()
        assert steps_meta_extract is None

        task_instance.run()

        step_meta = bq_extractor.extract_on_complete(task_instance)

        assert step_meta.run_facets['bigQuery_error'] == BigQueryErrorRunFacet(
            clientError=mock.ANY
        )
        mock_client.return_value.get_job.assert_called_once_with(job_id=bq_job_id)

        assert step_meta.inputs is not None
        assert len(step_meta.inputs) == 0
        assert step_meta.outputs is not None
        assert len(step_meta.outputs) == 0

        mock_client.return_value.close.assert_called()


class TestBigQueryExtractor(unittest.TestCase):
    def setUp(self):
        log.debug("TestBigQueryExtractor.setup(): ")
        self.task = TestBigQueryExtractor._get_bigquery_task()
        self.ti = TestBigQueryExtractor._get_ti(task=self.task)
        self.bq_extractor = BigQueryExtractor(operator=self.task)

    def test_extract(self):
        log.info("test_extractor")
        steps_meta_extract = BigQueryExtractor(self.task).extract()
        assert steps_meta_extract is None

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
        task_instance = TaskInstance(
            task=task,
            execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.RUNNING)
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
        task = BigQueryOperator(
            sql='select first_name, last_name from customers;',
            task_id="task_id",
            project_id="project_id",
            dag_id="dag_id",
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0)
        )

        return task


if __name__ == '__main__':
    unittest.main()
