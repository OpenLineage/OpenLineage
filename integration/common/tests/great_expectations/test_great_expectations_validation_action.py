# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import os
import sqlite3
import tempfile

import pandas
import pytest
from openlineage.common.provider.great_expectations import OpenLineageValidationAction
from sqlalchemy import create_engine

from great_expectations import get_context
from great_expectations.core import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
    RunIdentifier,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

current_env = os.environ


TABLE_NAME = "test_data"

# Common validation results
table_result = ExpectationValidationResult(
    success=True,
    expectation_config=ExpectationConfiguration(type="expect_table_row_count_to_equal", kwargs={"value": 10}),
    result={"observed_value": 10},
)
column_result = ExpectationValidationResult(
    success=True,
    expectation_config=ExpectationConfiguration(
        type="expect_column_sum_to_be_between",
        kwargs={"column": "size", "min_value": 0, "max_value": 100},
    ),
    result={"observed_value": 60},
)
result_suite = ExpectationSuiteValidationResult(
    success=True,
    suite_name="test_suite",
    results=[table_result, column_result],
    meta={
        "great_expectations_version": "1.2.3",
        "expectation_suite_name": "test_suite",
        "run_id": RunIdentifier(run_name="test_run"),
        "validation_time": datetime.datetime.now().isoformat(),
        "batch_kwargs": {},
    },
)

validation_identifier = ValidationResultIdentifier(
    ExpectationSuiteIdentifier("unit_test"),
    RunIdentifier(run_name="test_run"),
    "batch_id",
)


@pytest.fixture(scope="session")
def test_db_file():
    fd, file = tempfile.mkstemp()
    conn = sqlite3.connect(file)
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE {TABLE_NAME} (name text, birthdate text, address text, size integer)")
    yield file
    os.remove(file)


def test_dataset_from_sql_source_v3_api(test_db_file, tmpdir):
    connection_url = f"sqlite:///{test_db_file}"

    # Create a modern GE v3 context
    context_root_dir = tmpdir / "gx"
    ctx = get_context(context_root_dir=str(context_root_dir), mode="ephemeral")

    # Create and save expectation suite
    suite = ExpectationSuite("test_suite")
    ctx.suites.add(suite)

    # Add datasource using modern v3 FluentDatasource API
    from great_expectations.datasource.fluent import SQLDatasource

    datasource = SQLDatasource(name="food_delivery_db", connection_string=connection_url)
    ctx.add_datasource(datasource=datasource)

    # Use modern GE v3 checkpoint API with manual validation
    # Add the table asset and create a validator
    asset = datasource.add_table_asset(name=TABLE_NAME, table_name=TABLE_NAME)
    batch_request = asset.build_batch_request()
    validator = ctx.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")

    # Run validation manually
    validation_result = validator.validate()

    # Create action and run it manually
    action = OpenLineageValidationAction(
        name="openlineage_test_action",
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        do_publish=False,
        job_name="test_expectations_job",
    )

    from great_expectations.core import (
        RunIdentifier,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )

    run_id = RunIdentifier(run_name="test_run")
    validation_identifier = ValidationResultIdentifier(
        ExpectationSuiteIdentifier("test_suite"),
        run_id,
        "batch_id",
    )

    ol_result = action._run(
        validation_result_suite=validation_result,
        validation_result_suite_identifier=validation_identifier,
        data_asset=validator,
    )

    assert len(ol_result["outputs"]) == 0
    assert len(ol_result["inputs"]) == 1
    input_ds = ol_result["inputs"][0]
    assert input_ds["namespace"] == "sqlite"
    assert input_ds["name"] == TABLE_NAME
    assert "dataSource" in input_ds["facets"]
    assert input_ds["facets"]["dataSource"]["name"] == "sqlite"
    assert input_ds["facets"]["dataSource"]["uri"] == "sqlite:///" + test_db_file

    assert "schema" in input_ds["facets"]
    assert len(input_ds["facets"]["schema"]["fields"]) == 4
    assert all(
        f in input_ds["facets"]["schema"]["fields"]
        for f in [
            {"name": "name", "type": "TEXT", "fields": []},
            {"name": "birthdate", "type": "TEXT", "fields": []},
            {"name": "address", "type": "TEXT", "fields": []},
            {"name": "size", "type": "INTEGER", "fields": []},
        ]
    )

    assert len(input_ds["inputFacets"]) == 3
    assert all(
        k in input_ds["inputFacets"]
        for k in ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )


def test_dataset_from_sql_custom_query_v3_api(test_db_file, tmpdir):
    connection_url = f"sqlite:///{test_db_file}"
    engine = create_engine(connection_url)
    engine.execute("""CREATE TABLE IF NOT EXISTS join_table (name text, workplace text, position text)""")
    custom_sql = f"""SELECT * FROM {TABLE_NAME} t INNER JOIN join_table j ON t.name=j.name"""

    # Create a modern GE v3 context
    context_root_dir = tmpdir / "gx"
    ctx = get_context(context_root_dir=str(context_root_dir), mode="ephemeral")

    # Create and save expectation suite
    suite = ExpectationSuite("test_suite")
    ctx.suites.add(suite)

    # Add datasource using modern v3 FluentDatasource API
    from great_expectations.datasource.fluent import SQLDatasource

    datasource = SQLDatasource(name="food_delivery_db", connection_string=connection_url)
    ctx.add_datasource(datasource=datasource)
    # Use modern GE v3 API with manual validation
    # Add both table assets and query asset
    datasource.add_table_asset(name=TABLE_NAME, table_name=TABLE_NAME)
    datasource.add_table_asset(name="join_table", table_name="join_table")
    query_asset = datasource.add_query_asset(name="sql_query", query=custom_sql)
    batch_request = query_asset.build_batch_request()
    validator = ctx.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")

    # Run validation manually
    validation_result = validator.validate()

    # Create action and run it manually
    action = OpenLineageValidationAction(
        name="openlineage_test_action",
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        do_publish=False,
        job_name="test_expectations_job",
    )

    from great_expectations.core import (
        RunIdentifier,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )

    run_id = RunIdentifier(run_name="test_run")
    validation_identifier = ValidationResultIdentifier(
        ExpectationSuiteIdentifier("test_suite"),
        run_id,
        "batch_id",
    )

    ol_result = action._run(
        validation_result_suite=validation_result,
        validation_result_suite_identifier=validation_identifier,
        data_asset=validator,
    )

    assert len(ol_result["outputs"]) == 0
    assert len(ol_result["inputs"]) == 2

    input_ds = next(ds for ds in ol_result["inputs"] if ds["name"] == TABLE_NAME)

    assert input_ds["namespace"] == "sqlite"
    assert input_ds["name"] == TABLE_NAME
    assert "dataSource" in input_ds["facets"]
    assert input_ds["facets"]["dataSource"]["name"] == "sqlite"
    assert input_ds["facets"]["dataSource"]["uri"] == "sqlite:///" + test_db_file

    assert "schema" in input_ds["facets"]
    assert len(input_ds["facets"]["schema"]["fields"]) == 4
    assert all(
        f in input_ds["facets"]["schema"]["fields"]
        for f in [
            {"name": "name", "type": "TEXT", "fields": []},
            {"name": "birthdate", "type": "TEXT", "fields": []},
            {"name": "address", "type": "TEXT", "fields": []},
            {"name": "size", "type": "INTEGER", "fields": []},
        ]
    )

    assert len(input_ds["inputFacets"]) == 3
    assert all(
        k in input_ds["inputFacets"]
        for k in ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )

    input_ds = next(ds for ds in ol_result["inputs"] if ds["name"] == "join_table")
    assert "schema" in input_ds["facets"]
    assert len(input_ds["facets"]["schema"]["fields"]) == 3
    assert all(
        f in input_ds["facets"]["schema"]["fields"]
        for f in [
            {"name": "name", "type": "TEXT", "fields": []},
            {"name": "workplace", "type": "TEXT", "fields": []},
            {"name": "position", "type": "TEXT", "fields": []},
        ]
    )
    assert len(input_ds["inputFacets"]) == 3
    assert all(
        k in input_ds["inputFacets"]
        for k in ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )


def test_dataset_from_pandas_source_v3_api(tmpdir):
    # Create test DataFrame
    test_data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "city": ["New York", "London", "Tokyo"],
    }
    df = pandas.DataFrame(test_data)

    # Create a modern GE v3 context
    context_root_dir = tmpdir / "gx"
    ctx = get_context(context_root_dir=str(context_root_dir), mode="ephemeral")

    # Create and save expectation suite
    suite = ExpectationSuite("test_suite")
    ctx.suites.add(suite)

    # Add datasource using modern v3 FluentDatasource API for Pandas in-memory
    from great_expectations.datasource.fluent import PandasDatasource

    datasource = PandasDatasource(name="pandas_datasource")
    ctx.add_datasource(datasource=datasource)

    # Add DataFrame asset and create validator
    asset = datasource.add_dataframe_asset(name="test_dataframe")
    batch_request = asset.build_batch_request(options={"dataframe": df})
    validator = ctx.get_validator(batch_request=batch_request, expectation_suite_name="test_suite")

    # Run validation manually
    validation_result = validator.validate()

    # Create action and run it manually
    action = OpenLineageValidationAction(
        name="openlineage_test_action",
        openlineage_host="http://localhost:5000",
        openlineage_namespace="test_ns",
        do_publish=False,
        job_name="test_expectations_job",
    )

    from great_expectations.core import (
        RunIdentifier,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )

    run_id = RunIdentifier(run_name="test_run")
    validation_identifier = ValidationResultIdentifier(
        ExpectationSuiteIdentifier("test_suite"),
        run_id,
        "batch_id",
    )

    ol_result = action._run(
        validation_result_suite=validation_result,
        validation_result_suite_identifier=validation_identifier,
        data_asset=validator,
    )

    assert len(ol_result["outputs"]) == 0
    assert len(ol_result["inputs"]) == 1
    input_ds = ol_result["inputs"][0]

    # For in-memory Pandas DataFrame, check namespace and name
    assert input_ds["namespace"] == "memory://pandas"  # The namespace includes the full connection URL
    assert input_ds["name"] == "dataframe"
    assert "dataSource" in input_ds["facets"]
    assert input_ds["facets"]["dataSource"]["name"] == "memory://pandas"
    assert input_ds["facets"]["dataSource"]["uri"] == "memory://pandas"

    assert "schema" in input_ds["facets"]
    assert len(input_ds["facets"]["schema"]["fields"]) == 3
    # Check that all expected columns are present
    field_names = [f["name"] for f in input_ds["facets"]["schema"]["fields"]]
    assert all(name in field_names for name in ["name", "age", "city"])

    assert len(input_ds["inputFacets"]) == 3
    assert all(
        k in input_ds["inputFacets"]
        for k in ["dataQuality", "greatExpectations_assertions", "dataQualityMetrics"]
    )
