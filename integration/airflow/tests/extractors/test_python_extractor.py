# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import inspect
import os
from unittest.mock import patch

from airflow.operators.python_operator import PythonOperator

from openlineage.airflow.extractors.example_dag import python_task_getcwd
from openlineage.airflow.extractors.python_extractor import PythonExtractor
from openlineage.client.facet import SourceCodeJobFacet


def callable():
    print(10)


CODE = "def callable():\n    print(10)\n"


def test_extract_source_code():
    code = inspect.getsource(callable)
    assert code == CODE


def test_extract_operator_code():
    operator = PythonOperator(task_id='taskid', python_callable=callable)
    extractor = PythonExtractor(operator)
    assert extractor.extract().job_facets['sourceCode'] == SourceCodeJobFacet("python", CODE)


def test_extract_dag_code():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("python", "<built-in function getcwd>")


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "True"})
def test_extract_dag_code_env_disables_on_true():
    extractor = PythonExtractor(python_task_getcwd)
    metadata = extractor.extract()
    assert metadata is not None
    assert "sourceCode" not in metadata.job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "asdftgeragdsfgawef"})
def test_extract_dag_code_env_does_not_disable_on_random_string():
    extractor = PythonExtractor(python_task_getcwd)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("python", "<built-in function getcwd>")
