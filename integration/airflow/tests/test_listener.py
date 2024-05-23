# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime as dt
import uuid
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.airflow.listener import (
    on_task_instance_failed,
    on_task_instance_running,
    on_task_instance_success,
)
from openlineage.airflow.utils import is_airflow_version_enough

from airflow.models import DAG, BaseOperator, DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.utils.dates import days_ago
from airflow.utils.state import State

if not is_airflow_version_enough("2.6.0"):
    from airflow.listeners.events import (
        register_task_instance_state_events,
        unregister_task_instance_state_events,
    )


class TemplateOperator(BaseOperator):
    template_fields = ["df"]

    def __init__(self, df, *args, **kwargs):
        self.df = df
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return self.df


def render_df():
    return pd.DataFrame({"col": [1, 2]})


@patch("airflow.models.TaskInstance.xcom_push")
@patch("airflow.models.BaseOperator.render_template")
def test_listener_does_not_change_task_instance(render_mock, xcom_push_mock):
    if not is_airflow_version_enough("2.6.0"):
        register_task_instance_state_events()
    render_mock.return_value = render_df()

    dag = DAG(
        "test",
        start_date=days_ago(1),
        user_defined_macros={"render_df": render_df},
        params={"df": render_df()},
    )
    t = TemplateOperator(task_id="template_op", dag=dag, do_xcom_push=True, df=dag.param("df"))
    run_id = str(uuid.uuid1())
    dag.create_dagrun(state=State.NONE, run_id=run_id)
    ti = TaskInstance(t, run_id=run_id)
    ti.check_and_change_state_before_execution()  # make listener hook on running event
    ti._run_raw_task()

    if not is_airflow_version_enough("2.6.0"):
        # we need to unregister hooks not to break BigQuery E2E tests
        unregister_task_instance_state_events()

    # check if task returns the same DataFrame
    pd.testing.assert_frame_equal(xcom_push_mock.call_args[1]["value"], render_df())

    # check if render_template method always get the same unrendered field
    assert not isinstance(render_mock.call_args[0][0], pd.DataFrame)


@patch("openlineage.airflow.listener.getboolean")
@patch("openlineage.airflow.listener.is_airflow_version_enough")
@patch("openlineage.airflow.listener.execute_in_thread")
def test_listener_chooses_direct_execution_when_env_variable(
    execute_in_thread, is_airflow_version_enough, getboolean
):
    getboolean.return_value = True
    is_airflow_version_enough.return_value = False
    is_called = False

    def call():
        nonlocal is_called
        is_called = True

    from openlineage.airflow.listener import execute

    execute(call)
    assert is_called


@patch("openlineage.airflow.utils.getboolean")
@patch("openlineage.airflow.listener.is_airflow_version_enough")
@patch("openlineage.airflow.listener.execute_in_thread")
def test_listener_chooses_thread_execution(execute_in_thread, is_airflow_version_enough, getboolean):
    getboolean.return_value = False
    is_airflow_version_enough.return_value = False
    is_called = False

    def call():
        nonlocal is_called
        is_called = True

    from openlineage.airflow.listener import execute

    execute(call)
    assert not is_called


@pytest.fixture
def task_instance():
    dag = Mock()
    dag.dag_id = "dag_id"
    dag.description = "Test DAG Description"
    dag.owner = "Test Owner"
    task = Mock(dag=dag)
    task.dag_id = dag.dag_id
    task.task_id = "task_id"
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = DagRun()
    task_instance.dag_run.dag = dag
    task_instance.dag_run.dag_id = dag.dag_id
    task_instance.dag_run.run_id = "dag_run_run_id"
    task_instance.dag_run.data_interval_start = None
    task_instance.dag_run.data_interval_end = None
    task_instance.dag_run.execution_date = "execution_date"
    task_instance.dag_id = dag.dag_id
    task_instance.start_date = dt.datetime(2023, 1, 1, 13, 1, 1)
    task_instance.end_date = dt.datetime(2023, 1, 3, 13, 1, 1)
    task_instance.execution_date = task_instance.dag_run.execution_date
    task_instance.next_method = None  # Ensure this is None to reach start_task
    task_instance.render_templates = Mock()
    return task_instance


@pytest.mark.parametrize(
    "state", (TaskInstanceState.RUNNING, TaskInstanceState.SUCCESS, TaskInstanceState.FAILED)
)
def test_task_instance_try_number_property(state):
    start_try_number = 1
    ti = TaskInstance(task=Mock())
    ti._try_number = start_try_number
    ti.state = state

    expected_try_number = start_try_number if ti.state == TaskInstanceState.RUNNING else start_try_number + 1
    assert ti.try_number == expected_try_number
    assert ti._try_number == start_try_number


@patch("copy.deepcopy")
@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_running_task_correctly_calls_adapter_build_dag_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, mock_copy, task_instance
):
    mock_task_holder.set_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()
    mock_copy.return_value = task_instance

    on_task_instance_running(None, task_instance, None)
    mock_adapter.build_dag_run_id.assert_called_once_with(
        dag_id="dag_id",
        execution_date="execution_date",
    )


@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_failed_task_correctly_calls_adapter_build_dag_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, task_instance
):
    mock_task_holder.get_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()

    on_task_instance_failed(None, task_instance, None)
    mock_adapter.build_dag_run_id.assert_called_once_with(
        dag_id="dag_id",
        execution_date="execution_date",
    )


@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_successful_task_correctly_calls_adapter_build_dag_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, task_instance
):
    mock_task_holder.get_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()

    on_task_instance_success(None, task_instance, None)
    mock_adapter.build_dag_run_id.assert_called_once_with(
        dag_id="dag_id",
        execution_date="execution_date",
    )


@pytest.mark.parametrize("state", TaskInstanceState)
@patch("copy.deepcopy")
@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_running_task_correctly_calls_adapter_build_ti_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, mock_copy, task_instance, state
):
    """Tests the OpenLineageListener's response when a task instance is in the running state.

    This test ensures that when an Airflow task instance transitions to the running state,
    the OpenLineageAdapter's `build_task_instance_run_id` method is called exactly once with the correct
    parameters derived from the task instance.
    """
    mock_task_holder.set_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()
    mock_copy.return_value = task_instance

    task_instance._try_number = 1
    task_instance.state = state

    on_task_instance_running(None, task_instance, None)
    mock_adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        execution_date="execution_date",
    )


@pytest.mark.parametrize("state", TaskInstanceState)
@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_failed_task_correctly_calls_adapter_build_ti_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, task_instance, state
):
    mock_task_holder.get_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()

    task_instance._try_number = 1
    task_instance.state = state

    on_task_instance_failed(None, task_instance, None)
    mock_adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        execution_date="execution_date",
    )


@pytest.mark.parametrize("state", TaskInstanceState)
@patch("openlineage.airflow.listener.extractor_manager")
@patch("openlineage.airflow.listener.task_holder")
@patch("openlineage.airflow.listener.OpenLineageAdapter")
def test_successful_task_correctly_calls_adapter_build_ti_run_id_method(
    mock_adapter, mock_task_holder, mock_extractor, task_instance, state
):
    mock_task_holder.get_task.return_value = None
    mock_extractor.extract_metadata.return_value = OperatorLineage()

    task_instance._try_number = 1
    task_instance.state = state

    on_task_instance_success(None, task_instance, None)
    mock_adapter.build_task_instance_run_id.assert_called_once_with(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        execution_date="execution_date",
    )
