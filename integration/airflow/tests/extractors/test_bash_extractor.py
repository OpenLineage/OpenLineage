import os
from unittest.mock import patch

from airflow.operators.bash_operator import BashOperator

from openlineage.airflow.extractors.bash_extractor import BashExtractor
from openlineage.airflow.extractors.example_dag import bash_task
from openlineage.client.facet import SourceCodeJobFacet


def test_extract_operator_bash_command():
    operator = BashOperator(task_id='taskid', bash_command="exit 0")
    extractor = BashExtractor(operator)
    assert extractor.extract().job_facets['sourceCode'] == SourceCodeJobFacet("bash", "exit 0")


def test_extract_dag_bash_command():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("bash", "ls -halt && exit 0")


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "True"})
def test_extract_dag_bash_command_env_disables_on_true():
    extractor = BashExtractor(bash_task)
    assert extractor.extract() is None


@patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE": "asdftgeragdsfgawef"})
def test_extract_dag_bash_command_env_does_not_disable_on_random_string():
    extractor = BashExtractor(bash_task)
    assert extractor.extract().job_facets['sourceCode'] == \
           SourceCodeJobFacet("bash", "ls -halt && exit 0")
