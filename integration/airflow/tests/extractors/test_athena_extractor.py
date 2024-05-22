# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
from unittest import mock

import pytest
from openlineage.airflow.extractors.athena_extractor import AthenaExtractor
from openlineage.airflow.utils import is_airflow_version_enough
from openlineage.client.facet_v2 import symlinks_dataset
from openlineage.common.dataset import Dataset, Field, Source
from openlineage.common.models import DbColumn, DbTableSchema
from openlineage.common.sql import DbTableMeta

from airflow import DAG

if is_airflow_version_enough("2.2.4"):
    from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.dates import days_ago

# Database info
DB_NAME = "AwsDataCatalog"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")
OUTPUT_LOCATION = "s3://bucket/output"
DB_TABLE_COLUMNS = [
    DbColumn(name="ID", type="int", ordinal_position=1),
    DbColumn(name="AMOUNT_OFF", type="int", ordinal_position=2),
    DbColumn(name="CUSTOMER_EMAIL", type="varchar", ordinal_position=3),
    DbColumn(name="STARTS_ON", type="timestamp", ordinal_position=4),
    DbColumn(name="ENDS_ON", type="timestamp", ordinal_position=5),
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME, table_name=DB_TABLE_NAME, columns=DB_TABLE_COLUMNS
)

# Airflow info
DAG_ID = "email_discounts"
DAG = dag = DAG(  # type: ignore
    dag_id=DAG_ID,
    schedule_interval="@weekly",
    default_args={
        "owner": "dag_owner",
        "depends_on_past": False,
        "start_date": days_ago(7),
        "email_on_failure": False,
        "email_on_retry": False,
        "email": ["dag_owner@example.com"],
    },
    description="Email discounts to customers that have experienced order delays daily",
)


def mock_hook_connection():
    class ClientConfig:
        region_name = "eu-west-1"

    class MockHook:
        _client_config = ClientConfig()

        @staticmethod
        def get_table_metadata(CatalogName, DatabaseName, TableName):
            f = open(file="tests/extractors/athena_metadata.json")
            athena_metadata = json.loads(f.read())
            f.close()
            return athena_metadata[TableName]

    return MockHook()


@pytest.mark.skipif(
    not is_airflow_version_enough("2.2.4"),
    reason="Airflow < 2.2.4",
)
def test_extract_select():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_conn",
        side_effect=mock_hook_connection,
    ):
        sql = f"SELECT * FROM {DB_SCHEMA_NAME}.{DB_TABLE_NAME.name};"
        task_id = "select"
        select_task = AthenaOperator(
            task_id=task_id,
            aws_conn_id="aws_conn_id",
            query=sql,
            database=DB_SCHEMA_NAME,
            output_location=OUTPUT_LOCATION,
            dag=dag,
        )

        input_source = Source(
            scheme="awsathena",
            authority="athena.eu-west-1.amazonaws.com",
            connection_url="awsathena://athena.eu-west-1.amazonaws.com",
        )

        custom_facets = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace="s3://bucket",
                        name="/discount/data/path/",
                        type="TABLE",
                    )
                ]
            )
        }

        expected_inputs = [
            Dataset(
                name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
                source=input_source,
                fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS],
                custom_facets=custom_facets,
            ).to_openlineage_dataset()
        ]

        output_source = Source(
            scheme="s3://bucket",
            connection_url="s3://bucket/output",
        )

        expected_outputs = [
            Dataset(
                name="/output",
                source=output_source,
            ).to_openlineage_dataset()
        ]

        task_metadata = AthenaExtractor(select_task).extract()

        assert task_metadata.name == f"{DAG_ID}.{task_id}"
        assert task_metadata.inputs == expected_inputs
        assert task_metadata.outputs == expected_outputs


@pytest.mark.skipif(
    not is_airflow_version_enough("2.2.4"),
    reason="Airflow < 2.2.4",
)
def test_extract_create():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_conn",
        side_effect=mock_hook_connection,
    ):
        sql = "CREATE EXTERNAL TABLE TEST_TABLE (column STRING)"
        create_task = AthenaOperator(
            task_id="create",
            aws_conn_id="aws_conn_id",
            query=sql,
            database=DB_SCHEMA_NAME,
            output_location="s3://bucket",
            dag=dag,
        )
        task_metadata = AthenaExtractor(create_task).extract()

        input_source = Source(
            scheme="awsathena",
            authority="athena.eu-west-1.amazonaws.com",
            connection_url="awsathena://athena.eu-west-1.amazonaws.com",
        )

        symlink_facets = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace="s3://bucket",
                        name="/data/test_table/data/path",
                        type="TABLE",
                    )
                ]
            )
        }

        first_expected_output = Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.TEST_TABLE",
            source=input_source,
            fields=[Field.from_column(DbColumn(name="column", type="string", ordinal_position=1))],
            custom_facets=symlink_facets,
        ).to_openlineage_dataset()

        assert task_metadata.inputs == []
        assert len(task_metadata.outputs) == 2
        assert task_metadata.outputs[0] == first_expected_output
        assert task_metadata.outputs[1].namespace == "s3://bucket"
        assert task_metadata.outputs[1].name == "/"
        assert task_metadata.job_facets["sql"].query == sql


@pytest.mark.skipif(
    not is_airflow_version_enough("2.2.4"),
    reason="Airflow < 2.2.4",
)
def test_extract_insert_select():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_conn",
        side_effect=mock_hook_connection,
    ):
        sql = "INSERT INTO TEST_TABLE SELECT CUSTOMER_EMAIL FROM DISCOUNTS"
        insert_select_task = AthenaOperator(
            task_id="insert_select",
            aws_conn_id="aws_conn_id",
            query=sql,
            database=DB_SCHEMA_NAME,
            output_location=OUTPUT_LOCATION,
            dag=dag,
        )
        task_metadata = AthenaExtractor(insert_select_task).extract()

        source = Source(
            scheme="awsathena",
            authority="athena.eu-west-1.amazonaws.com",
            connection_url="awsathena://athena.eu-west-1.amazonaws.com",
        )

        input_facets = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace="s3://bucket",
                        name="/discount/data/path/",
                        type="TABLE",
                    )
                ]
            )
        }

        expected_inputs = [
            Dataset(
                name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
                source=source,
                fields=[Field.from_column(column) for column in DB_TABLE_COLUMNS],
                custom_facets=input_facets,
            ).to_openlineage_dataset()
        ]

        output_facets = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace="s3://bucket",
                        name="/data/test_table/data/path",
                        type="TABLE",
                    )
                ]
            )
        }

        first_expected_output = Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.TEST_TABLE",
            source=source,
            fields=[Field.from_column(DbColumn(name="column", type="string", ordinal_position=1))],
            custom_facets=output_facets,
        ).to_openlineage_dataset()

        assert task_metadata.inputs == expected_inputs
        assert len(task_metadata.outputs) == 2
        assert task_metadata.outputs[0] == first_expected_output
        assert task_metadata.outputs[1].namespace == "s3://bucket"
        assert task_metadata.outputs[1].name == "/output"
        assert task_metadata.job_facets["sql"].query == sql


@pytest.mark.skipif(
    not is_airflow_version_enough("2.2.4"),
    reason="Airflow < 2.2.4",
)
def test_extract_drop():
    with mock.patch(
        "airflow.providers.amazon.aws.hooks.athena.AthenaHook.get_conn",
        side_effect=mock_hook_connection,
    ):
        sql = "DROP TABLE TEST_TABLE"
        create_task = AthenaOperator(
            task_id="drop",
            aws_conn_id="aws_conn_id",
            query=sql,
            database=DB_SCHEMA_NAME,
            output_location=OUTPUT_LOCATION,
            dag=dag,
        )
        task_metadata = AthenaExtractor(create_task).extract()

        input_source = Source(
            scheme="awsathena",
            authority="athena.eu-west-1.amazonaws.com",
            connection_url="awsathena://athena.eu-west-1.amazonaws.com",
        )

        symlink_facets = {
            "symlinks": symlinks_dataset.SymlinksDatasetFacet(
                identifiers=[
                    symlinks_dataset.Identifier(
                        namespace="s3://bucket",
                        name="/data/test_table/data/path",
                        type="TABLE",
                    )
                ]
            )
        }

        first_expected_output = Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.TEST_TABLE",
            source=input_source,
            fields=[Field.from_column(DbColumn(name="column", type="string", ordinal_position=1))],
            custom_facets=symlink_facets,
        ).to_openlineage_dataset()

        assert task_metadata.inputs == []
        assert len(task_metadata.outputs) == 2
        assert task_metadata.outputs[0] == first_expected_output
        assert task_metadata.outputs[1].namespace == "s3://bucket"
        assert task_metadata.outputs[1].name == "/output"
        assert task_metadata.job_facets["sql"].query == sql
