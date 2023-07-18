# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import random
import unittest
import uuid
from datetime import datetime
from unittest import mock

import pytest
import pytz
from mock import PropertyMock
from openlineage.airflow.extractors.redshift_data_extractor import RedshiftDataExtractor
from openlineage.client.facet import ErrorMessageRunFacet, OutputStatisticsOutputDatasetFacet
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta
from pkg_resources import parse_version

from airflow.models import DAG, TaskInstance
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION

CONN_ID = "food_delivery_db"
CONN_URI = (
    "redshift://user:pass@redshift-cluster-name.id.region.redshift.amazonaws.com:5439"
    "/food_delivery"
)
CONN_URI_WITHOUT_USERPASS = \
    "redshift://redshift-cluster-name.id.region.redshift.amazonaws.com:5439/food_delivery"

DB_NAME = "food_delivery"
DB_SCHEMA_NAME = "public"
DB_TABLE_NAME = DbTableMeta("discounts")
DB_TABLE_COLUMNS = [
    DbColumn(name="id", type="int4", ordinal_position=1),
    DbColumn(name="amount_off", type="int4", ordinal_position=2),
    DbColumn(name="customer_email", type="varchar", ordinal_position=3),
    DbColumn(name="starts_on", type="timestamp", ordinal_position=4),
    DbColumn(name="ends_on", type="timestamp", ordinal_position=5),
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []

REDSHIFT_DATABASE = "dev"
REDSHIFT_DATABASE_USER = "admin"
CLUSTER_IDENTIFIER = "redshift-cluster-1"
REGION_NAME = "eu-west-2"

REDSHIFT_QUERY = """
CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
            """

log = logging.getLogger(__name__)


class TestRedshiftDataExtractor(unittest.TestCase):
    def setUp(self):
        log.debug("TestRedshiftDataExtractor.setup(): ")
        run_id = str(uuid.uuid4())
        self.task = TestRedshiftDataExtractor._get_redshift_task(run_id)
        self.ti = TestRedshiftDataExtractor._get_ti(task=self.task, run_id=run_id)
        self.extractor = RedshiftDataExtractor(operator=self.task)

    def test_extract(self):
        log.info("test_extractor")
        tasks_meta_extract = RedshiftDataExtractor(self.task).extract()
        assert tasks_meta_extract is None

    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_get_xcom_redshift_job_id(self, mock_xcom_pull):
        self.extractor._get_xcom_redshift_job_id(self.ti)

        mock_xcom_pull.assert_called_once_with(task_ids=self.ti.task_id)

    @staticmethod
    def _get_ti(task, run_id):
        kwargs = {}
        if parse_version(AIRFLOW_VERSION) > parse_version("2.2.0"):
            kwargs["run_id"] = run_id
        else:
            kwargs["execution_date"] = datetime.utcnow().replace(tzinfo=pytz.utc)
        task_instance = TaskInstance(
            task=task,
            # execution_date=datetime.utcnow().replace(tzinfo=pytz.utc),
            state=State.RUNNING,
            **kwargs,
        )
        task_instance.job_id = random.randrange(10000)

        return task_instance

    @staticmethod
    def _get_redshift_task(run_id):
        dag = DAG(dag_id="TestRedshiftDataExtractor")
        execution_date = datetime.utcnow().replace(tzinfo=pytz.utc)
        dag.create_dagrun(
            run_id=run_id,
            state=State.QUEUED,
            execution_date=execution_date,
        )
        task = RedshiftDataOperator(
            task_id="task_id",
            database=REDSHIFT_DATABASE,
            db_user=REDSHIFT_DATABASE_USER,
            cluster_identifier=CLUSTER_IDENTIFIER,
            sql=REDSHIFT_QUERY,
            region=REGION_NAME,
            dag=dag,
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
        )

        if hasattr(task, "return_sql_result"):
            task.return_sql_result = True

        return task

    @mock.patch(
        "airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.hook",
        new_callable=PropertyMock,
    )
    @mock.patch("botocore.client")
    @mock.patch("airflow.models.TaskInstance.xcom_pull")
    def test_extract_e2e(self, xcom_pull, mock_client, mock_hook):
        xcom_pull.return_value = "test_id"
        mock_client.describe_statement.return_value = self.read_file_json(
            "tests/extractors/redshift_statement_details.json"
        )
        mock_client.describe_table.return_value = self.read_file_json(
            "tests/extractors/redshift_table_details.json"
        )
        job_id = "test_id"
        mock_client.execute_statement.return_value = {"Id": job_id}
        mock_client.get_statement_result.return_value = None
        mock_hook.return_value.conn = mock_client

        extractor = RedshiftDataExtractor(self.task)
        task_meta_extract = extractor.extract()
        assert task_meta_extract is None

        self.ti.run()

        task_meta = extractor.extract_on_complete(self.ti)

        mock_client.describe_statement.assert_called_with(Id=job_id)

        assert task_meta.outputs
        assert len(task_meta.outputs) == 1
        assert task_meta.outputs[0].name == "dev.public.fruit"

        assert task_meta.outputs[0].facets["schema"].fields is not None
        assert (
            task_meta.outputs[0].facets["dataSource"].name
            == f"redshift://{CLUSTER_IDENTIFIER}.{REGION_NAME}:5439"
        )
        assert task_meta.outputs[0].facets["dataSource"].uri == ""
        assert len(task_meta.outputs[0].facets["schema"].fields) == 3

        assert (
            OutputStatisticsOutputDatasetFacet(
                rowCount=1,
                size=11,
            ) == task_meta.outputs[0].facets['stats']
        )

    @pytest.mark.skipif(
        parse_version(AIRFLOW_VERSION) >= parse_version("2.5.0"),
        reason="Airflow >= 2.5.0",
    )
    @mock.patch(
        "airflow.providers.amazon.aws.operators.redshift_data."
        "RedshiftDataOperator.wait_for_results"
    )
    @mock.patch(
        "airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator.hook",
        new_callable=PropertyMock,
    )
    @mock.patch("botocore.client")
    def test_extract_error(self, mock_client, mock_hook, mock_wait_for_results):
        mock_client.describe_statement.side_effect = Exception("redshift error")
        mock_client.describe_table.side_effect = Exception(
            "redshift error on describe table"
        )
        mock_wait_for_results.return_value = True
        job_id = "test_id"
        mock_client.execute_statement.return_value = {"Id": job_id}
        mock_hook.return_value.conn = mock_client

        extractor = RedshiftDataExtractor(self.task)
        task_meta_extract = extractor.extract()
        assert task_meta_extract is None

        self.ti.run()

        task_meta = extractor.extract_on_complete(self.ti)

        mock_client.describe_statement.assert_called_with(Id=job_id)

        assert isinstance(task_meta.run_facets["errorMessage"], ErrorMessageRunFacet)
        assert (
            task_meta.run_facets["errorMessage"].message
            == "Cannot retrieve job details from Redshift Data client. redshift error"
        )
        assert task_meta.run_facets["errorMessage"].programmingLanguage == "PYTHON"
        assert task_meta.run_facets["errorMessage"].stackTrace.startswith(
            "redshift error: Traceback (most recent call last):\n"
        )
        assert len(task_meta.outputs) == 1
        assert task_meta.outputs[0].name == "fruit"

        assert "schema" not in task_meta.outputs[0].facets
        assert (
            task_meta.outputs[0].facets["dataSource"].name
            == f"redshift://{CLUSTER_IDENTIFIER}.{REGION_NAME}:5439"
        )
        assert task_meta.outputs[0].facets["dataSource"].uri == ""

    def read_file_json(self, file):
        f = open(file=file, mode="r")
        details = json.loads(f.read())
        f.close()
        return details
