# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import json
import os
import sqlite3
import tempfile
from unittest.mock import patch

import pandas
import pytest
from great_expectations.core import (
  ExpectationSuiteValidationResult,
  ExpectationValidationResult,
  ExpectationConfiguration, RunIdentifier,
)
from great_expectations.data_context.types.resource_identifiers import (
  ExpectationSuiteIdentifier, ValidationResultIdentifier,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.checkpoint.checkpoint import (
  Checkpoint,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
  DataContextConfig,
  FilesystemStoreBackendDefaults,
)
from great_expectations.dataset import SqlAlchemyDataset, PandasDataset
from openlineage.client.facet import SchemaField
from sqlalchemy import create_engine

from openlineage.common.provider.great_expectations import \
  OpenLineageValidationAction
from openlineage.common.provider.great_expectations.results import (
  GreatExpectationsAssertion,
)

current_env = os.environ

project_config = DataContextConfig(
    datasources={
      "food_delivery_db": {
        "data_asset_type": {
          "module_name": "great_expectations.dataset",
          "class_name": "SqlAlchemyDataset",
        },
        "class_name": "SqlAlchemyDatasource",
        "module_name": "great_expectations.datasource",
        "credentials": {"url": "bigquery://openlineage/food_delivery"},
      },
      "gcs": {
        "data_asset_type": {
          "class_name": "PandasDataset",
          "module_name": "great_expectations.dataset",
        },
        "class_name": "PandasDatasource",
        "module_name": "great_expectations.datasource",
      },
    },
    validation_operators={
      "action_list_operator": {
        "class_name": "ActionListValidationOperator",
        "action_list": [
          {
            "name": "openlineage",
            "action": {
              "class_name": "OpenLineageValidationAction",
              "module_name": "openlineage.common.provider.great_expectations.action",
            },
          }
        ],
      }
    },
    anonymous_usage_statistics={"enabled": False},
)

TABLE_NAME = "test_data"

# Common validation results
table_result = ExpectationValidationResult(
    success=True,
    expectation_config=ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal", kwargs={"value": 10}
    ),
    result={"observed_value": 10},
)
column_result = ExpectationValidationResult(
    success=True,
    expectation_config=ExpectationConfiguration(
        expectation_type="expect_column_sum_to_be_between",
        kwargs={"column": "size", "min_value": 0, "max_value": 100},
    ),
    result={"observed_value": 60},
)
result_suite = ExpectationSuiteValidationResult(
    success=True,
    meta={'great_expectations_version': '1.2.3',
          'expectation_suite_name': 'test_suite',
          'run_id': RunIdentifier(run_name="test_run"),
          'validation_time': datetime.datetime.now().isoformat(),
          "batch_kwargs": {}},
    results=[table_result, column_result],
)

validation_identifier = ValidationResultIdentifier(
    ExpectationSuiteIdentifier(expectation_suite_name="unit_test"),
    RunIdentifier(run_name="test_run"),
    "batch_id")


@pytest.fixture(scope="session")
def test_db_file():
    fd, file = tempfile.mkstemp()
    conn = sqlite3.connect(file)
    cursor = conn.cursor()
    cursor.execute(
        f"CREATE TABLE {TABLE_NAME} (name text, birthdate text, address text, size integer)"
    )
    yield file
    os.remove(file)


@patch.dict(os.environ,
            {"OPENLINEAGE_URL": "http://localhost:5000", **current_env})
def test_dataset_from_sql_source(test_db_file, tmpdir):
    connection_url = f"sqlite:///{test_db_file}"
    engine = create_engine(connection_url)

    ds = SqlAlchemyDataset(table_name=TABLE_NAME, engine=engine)

    store_defaults = FilesystemStoreBackendDefaults(root_directory=tmpdir)
    project_config.stores = store_defaults.stores
    project_config.expectations_store_name = store_defaults.expectations_store_name
    project_config.validations_store_name = store_defaults.validations_store_name
    project_config.checkpoint_store_name = store_defaults.checkpoint_store_name

    ctx = BaseDataContext(project_config=project_config)
    action = OpenLineageValidationAction(
        ctx,
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        job_name="test_job",
        do_publish=False
    )
    run_event = action.run(validation_result_suite=result_suite,
                           validation_result_suite_identifier=validation_identifier,
                           data_asset=ds)
    datasets = run_event["inputs"]
    assert datasets is not None
    assert len(datasets) == 1
    input_ds = datasets[0]
    assert input_ds['name'] == TABLE_NAME
    assert input_ds['namespace'] == "sqlite"

    assert "dataSource" in input_ds['facets']
    assert input_ds['facets']["dataSource"]['name'] == "sqlite"
    assert input_ds['facets']["dataSource"]['uri'] == "sqlite:/" + test_db_file

    assert "schema" in input_ds['facets']
    assert len(input_ds['facets']['schema']['fields']) == 4
    assert all(
        f in input_ds['facets']['schema']['fields']
        for f in [
          {"name": "name", "type": "TEXT"},
          {"name": "birthdate", "type": "TEXT"},
          {"name": "address", "type": "TEXT"},
          {"name": "size", "type": "INTEGER"},
        ]
    )

    assert len(input_ds['inputFacets']) == 3
    assert all(
        k in input_ds['inputFacets']
        for k in
        ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )
    assert input_ds['inputFacets']["dataQuality"]['rowCount'] == 10
    assert "size" in input_ds['inputFacets']["dataQuality"]['columnMetrics']
    assert input_ds['inputFacets']["dataQuality"]['columnMetrics']["size"][
             'sum'] == 60

    assert len(
        input_ds['inputFacets']["greatExpectations_assertions"][
          'assertions']) == 2
    assert all(
        a in input_ds['inputFacets']["greatExpectations_assertions"]['assertions']
        for a in [
          {"expectationType": "expect_table_row_count_to_equal",
           "success": True},
          {"expectationType": "expect_column_sum_to_be_between", "success": True,
           "column": "size"},
        ]
    )


@patch.dict(os.environ,
            {"OPENLINEAGE_URL": "http://localhost:5000", **current_env})
def test_dataset_from_custom_sql(test_db_file, tmpdir):
    connection_url = f"sqlite:///{test_db_file}"
    engine = create_engine(connection_url)
    engine.execute(
        """CREATE TABLE join_table (name text, workplace text, position text)"""
    )
    custom_sql = (
      f"""SELECT * FROM {TABLE_NAME} t INNER JOIN join_table j ON t.name=j.name"""
    )

    # note the batch_kwarg key is 'query', but the constructor arg is 'custom_sql'
    ds = SqlAlchemyDataset(
        engine=engine, custom_sql=custom_sql, batch_kwargs={"query": custom_sql}
    )

    store_defaults = FilesystemStoreBackendDefaults(root_directory=tmpdir)
    project_config.stores = store_defaults.stores
    project_config.expectations_store_name = store_defaults.expectations_store_name
    project_config.validations_store_name = store_defaults.validations_store_name
    project_config.checkpoint_store_name = store_defaults.checkpoint_store_name

    ctx = BaseDataContext(project_config=project_config)
    action = OpenLineageValidationAction(
        ctx,
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        job_name="test_job",
    )
    datasets = action._fetch_datasets_from_sql_source(ds, result_suite)
    assert datasets is not None
    assert len(datasets) == 2
    assert all(
        name in [TABLE_NAME, "join_table"] for name in
        [ds.name for ds in datasets]
    )

    input_ds = next(ds for ds in datasets if ds.name == TABLE_NAME)

    assert "dataSource" in input_ds.facets
    assert input_ds.facets["dataSource"].name == "sqlite"
    assert input_ds.facets["dataSource"].uri == "sqlite:/" + test_db_file

    assert "schema" in input_ds.facets
    assert len(input_ds.facets["schema"].fields) == 4
    assert all(
        f in input_ds.facets["schema"].fields
        for f in [
          SchemaField("name", "TEXT"),
          SchemaField("birthdate", "TEXT"),
          SchemaField("address", "TEXT"),
          SchemaField("size", "INTEGER"),
        ]
    )
    assert len(input_ds.inputFacets) == 3
    assert all(
        k in input_ds.inputFacets
        for k in
        ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )
    assert input_ds.inputFacets["dataQuality"].rowCount == 10
    assert "size" in input_ds.inputFacets["dataQuality"].columnMetrics
    assert input_ds.inputFacets["dataQuality"].columnMetrics["size"].sum == 60

    assert len(
        input_ds.inputFacets["greatExpectations_assertions"].assertions) == 2
    assert all(
        a in input_ds.inputFacets["greatExpectations_assertions"].assertions
        for a in [
          GreatExpectationsAssertion("expect_table_row_count_to_equal", True),
          GreatExpectationsAssertion("expect_column_sum_to_be_between", True,
                                     "size"),
        ]
    )

    input_ds = next(ds for ds in datasets if ds.name == "join_table")
    assert "schema" in input_ds.facets
    assert len(input_ds.facets["schema"].fields) == 3
    assert all(
        f in input_ds.facets["schema"].fields
        for f in [
          SchemaField("name", "TEXT"),
          SchemaField("workplace", "TEXT"),
          SchemaField("position", "TEXT"),
        ]
    )
    assert len(input_ds.inputFacets) == 3
    assert all(
        k in input_ds.inputFacets
        for k in
        ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )


@patch.dict(os.environ,
            {"OPENLINEAGE_URL": "http://localhost:5000", **current_env})
def test_dataset_from_pandas_source(tmpdir):
    data_file = tmpdir + "/data.json"
    json_data = [
      {
        "name": "my name",
        "birthdate": "2020-10-01",
        "address": "1234 Main st",
        "size": 12,
      },
      {
        "name": "your name",
        "birthdate": "2020-06-01",
        "address": "1313 Mockingbird Ln",
        "size": 12,
      },
    ]
    with open(data_file, mode="w") as out:
        json.dump(json_data, out)

    store_defaults = FilesystemStoreBackendDefaults(root_directory=tmpdir)
    project_config.stores = store_defaults.stores
    project_config.expectations_store_name = store_defaults.expectations_store_name
    project_config.validations_store_name = store_defaults.validations_store_name
    project_config.checkpoint_store_name = store_defaults.checkpoint_store_name

    ctx = BaseDataContext(project_config=project_config)
    pd_dataset = PandasDataset(
        pandas.read_json(data_file),
        **{
          "batch_kwargs": {"path": "gcs://my_bucket/path/to/my/data"},
          "data_context": ctx,
        },
    )
    action = OpenLineageValidationAction(
        ctx,
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        job_name="test_job",
    )

    datasets = action._fetch_datasets_from_pandas_source(
        pd_dataset, validation_result_suite=result_suite
    )
    assert len(datasets) == 1
    input_ds = datasets[0]
    assert input_ds.name == "/path/to/my/data"
    assert input_ds.namespace == "gcs://my_bucket"

    assert "dataSource" in input_ds.facets
    assert input_ds.facets["dataSource"].name == "gcs://my_bucket"
    assert input_ds.facets["dataSource"].uri == "gcs://my_bucket"

    assert "schema" in input_ds.facets
    assert len(input_ds.facets["schema"].fields) == 4
    assert all(
        f in input_ds.facets["schema"].fields
        for f in [
          SchemaField("name", "object"),
          SchemaField("birthdate", "object"),
          SchemaField("address", "object"),
          SchemaField("size", "int64"),
        ]
    )

    assert len(input_ds.inputFacets) == 3
    assert all(
        k in input_ds.inputFacets
        for k in
        ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )
    assert input_ds.inputFacets["dataQuality"].rowCount == 10
    assert "size" in input_ds.inputFacets["dataQuality"].columnMetrics
    assert input_ds.inputFacets["dataQuality"].columnMetrics["size"].sum == 60

    assert len(
        input_ds.inputFacets["greatExpectations_assertions"].assertions) == 2
    assert all(
        a in input_ds.inputFacets["greatExpectations_assertions"].assertions
        for a in [
          GreatExpectationsAssertion("expect_table_row_count_to_equal", True),
          GreatExpectationsAssertion("expect_column_sum_to_be_between", True,
                                     "size"),
        ]
    )


def test_dataset_from_sql_source_v3_api(test_db_file, tmpdir):
    connection_url = f"sqlite:///{test_db_file}"

    store_defaults = FilesystemStoreBackendDefaults(root_directory=tmpdir)
    store_defaults.stores[store_defaults.expectations_store_name][
      "store_backend"] = {
      "class_name": "InMemoryStoreBackend"
    }
    project_config.stores = store_defaults.stores
    project_config.expectations_store_name = store_defaults.expectations_store_name
    project_config.validations_store_name = store_defaults.validations_store_name
    project_config.checkpoint_store_name = store_defaults.checkpoint_store_name
    project_config.evaluation_parameter_store_name = (
      store_defaults.evaluation_parameter_store_name
    )

    ctx = BaseDataContext(project_config=project_config)
    ctx.stores[store_defaults.expectations_store_name].set(
        ExpectationSuiteIdentifier("test_suite"), ExpectationSuite("test_suite")
    )

    ctx.add_datasource(
        "food_delivery_db",
        True,
        class_name="SimpleSqlalchemyDatasource",
        connection_string=connection_url,
        tables={
          TABLE_NAME: {
            "partitioners": {"sql_table": {"include_schema_name": "false"}}
          }
        },
        dry_run=True,
    )
    checkpoint: Checkpoint = Checkpoint(
        "test_checkpoint",
        ctx,
        config_version=1,
        expectation_suite_name="test_suite",
        validation_operator_name="action_list_operator",
    )
    result = checkpoint.run(
        run_name_template="the_run",
        expectation_suite_name="test_suite",
        validations=[
          {
            "batch_request": {
              "datasource_name": "food_delivery_db",
              "data_connector_name": "sql_table",
              "data_asset_name": TABLE_NAME,
            },
            "expectations_suite_name": "test_suite",
            "action_list": [
              {
                "name": "openlineage",
                "action": {
                  "class_name": OpenLineageValidationAction.__name__,
                  "module_name": OpenLineageValidationAction.__module__,
                  "do_publish": False,
                  "job_name": "test_expectations_job",
                },
              }
            ],
          }
        ],
    )

    assert len(result.run_results) == 1
    validation_id = next(iter(result.run_results))
    ol_result = result.run_results[validation_id]["actions_results"][
      "openlineage"]
    assert len(ol_result["outputs"]) == 0
    assert len(ol_result["inputs"]) == 1
    input_ds = ol_result["inputs"][0]
    assert input_ds["namespace"] == "sqlite"
    assert input_ds["name"] == TABLE_NAME
    assert "dataSource" in input_ds["facets"]
    assert input_ds["facets"]["dataSource"]["name"] == "sqlite"
    assert input_ds["facets"]["dataSource"]["uri"] == "sqlite:/" + test_db_file

    assert "schema" in input_ds["facets"]
    assert len(input_ds["facets"]["schema"]["fields"]) == 4
    assert all(
        f in input_ds["facets"]["schema"]["fields"]
        for f in [
          {"name": "name", "type": "TEXT"},
          {"name": "birthdate", "type": "TEXT"},
          {"name": "address", "type": "TEXT"},
          {"name": "size", "type": "INTEGER"},
        ]
    )

    assert len(input_ds["inputFacets"]) == 3
    assert all(
        k in input_ds["inputFacets"]
        for k in
        ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )
