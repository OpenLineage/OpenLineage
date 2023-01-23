# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
from unittest.mock import patch

from openlineage.airflow.extractors.bash_extractor import BashExtractor
from openlineage.airflow.extractors.example_dag import bash_task
from openlineage.client.facet import SourceCodeJobFacet

from airflow.operators.bash_operator import BashOperator


def test_extract_operator_bash_command_disables_without_env():
    operator = BashOperator(task_id='taskid', bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert 'sourceCode' not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_operator_bash_command_enables_on_true():
    operator = BashOperator(task_id='taskid', bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert extractor.extract().job_facets['sourceCode'] == SourceCodeJobFacet("bash", "exit 0")


@patch.dict(
    os.environ,
    {
        k: v
        for k, v in os.environ.items()
        if k != "OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE"
    },
    clear=True,
)
def test_extract_dag_bash_command_disabled_without_env():
    extractor = BashExtractor(bash_task)
    assert "sourceCode" not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "False"})
def test_extract_dag_bash_command_enables_on_true():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("bash", "ls -halt && exit 0")


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "True"})
def test_extract_dag_bash_command_env_disables_on_true():
    extractor = BashExtractor(bash_task)
    assert 'sourceCode' not in extractor.extract().job_facets


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "asdftgeragdsfgawef"})
def test_extract_dag_bash_command_env_does_not_disable_on_random_string():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("bash", "ls -halt && exit 0")
