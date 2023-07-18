# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import uuid
from unittest.mock import patch

import pandas as pd
from openlineage.airflow.utils import is_airflow_version_enough

if not is_airflow_version_enough("2.6.0"):
    from airflow.listeners.events import (
        register_task_instance_state_events,
        unregister_task_instance_state_events,
    )
from airflow.models import DAG, BaseOperator, TaskInstance
from airflow.utils.dates import days_ago
from airflow.utils.state import State


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
    t = TemplateOperator(
        task_id="template_op", dag=dag, do_xcom_push=True, df=dag.param("df")
    )
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
def test_listener_chooses_thread_execution(
        execute_in_thread, is_airflow_version_enough, getboolean
):
    getboolean.return_value = False
    is_airflow_version_enough.return_value = False
    is_called = False

    def call():
        nonlocal is_called
        is_called = True

    from openlineage.airflow.listener import execute

    execute(call)
    assert not is_called
