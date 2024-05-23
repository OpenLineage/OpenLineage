# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timezone
from unittest import mock

from openlineage.airflow.adapter import _DAG_NAMESPACE
from openlineage.airflow.macros import (
    lineage_job_name,
    lineage_job_namespace,
    lineage_parent_id,
    lineage_run_id,
)


def test_lineage_job_namespace():
    assert lineage_job_namespace() == _DAG_NAMESPACE


def test_lineage_job_name():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=mock.MagicMock(run_id="run_id"),
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
        try_number=1,
    )
    assert lineage_job_name(task_instance) == "dag_id.task_id"


def test_lineage_run_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=mock.MagicMock(run_id="run_id"),
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
        try_number=1,
    )

    call_result1 = lineage_run_id(task_instance)
    call_result2 = lineage_run_id(task_instance)

    # random part value does not matter, it just have to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("016f5e9e-c4c8-")


def test_lineage_parent_id():
    task_instance = mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        dag_run=mock.MagicMock(run_id="run_id"),
        execution_date=datetime(2020, 1, 1, 1, 1, 1, 0, tzinfo=timezone.utc),
        try_number=1,
    )

    call_result1 = lineage_parent_id(task_instance)
    call_result2 = lineage_parent_id(task_instance)

    # random part value does not matter, it just have to be the same for the same TaskInstance
    assert call_result1 == call_result2
    # execution_date is used as most significant bits of UUID
    assert call_result1.startswith("default/dag_id.task_id/016f5e9e-c4c8-")
