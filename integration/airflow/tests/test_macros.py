# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import uuid
from unittest import mock

from openlineage.airflow.adapter import _DAG_NAMESPACE
from openlineage.airflow.macros import lineage_parent_id, lineage_run_id


def test_lineage_run_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        execution_date="execution_date",
        try_number=1,
    )
    task = mock.MagicMock(task_id="task_id")
    actual = lineage_run_id(task=task, task_instance=task_instance)
    expected = str(
        uuid.uuid3(
            uuid.NAMESPACE_URL,
            f"{_DAG_NAMESPACE}.dag_id.task_id.execution_date.1",
        )
    )
    assert actual == expected


def test_lineage_parent_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        execution_date="execution_date",
        try_number=1,
    )
    task = mock.MagicMock(task_id="task_id")
    actual = lineage_parent_id(run_id="run_id", task_instance=task_instance, task=task)
    job_name = str(
        uuid.uuid3(
            uuid.NAMESPACE_URL,
            f"{_DAG_NAMESPACE}.dag_id.task_id.execution_date.1",
        )
    )
    expected = f"{_DAG_NAMESPACE}/{job_name}/run_id"
    assert actual == expected
