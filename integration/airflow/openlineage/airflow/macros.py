# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import typing

from openlineage.airflow.adapter import OpenLineageAdapter

if typing.TYPE_CHECKING:
    from airflow.models import BaseOperator, TaskInstance

_JOB_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "default")


def lineage_run_id(task: "BaseOperator", task_instance: "TaskInstance"):
    """
    Macro function which returns the generated run id for a given task. This
    can be used to forward the run id from a task to a child run so the job
    hierarchy is preserved. Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ lineage_run_id(task, task_instance) }}'], # lineage_run_id macro invoked
        provide_context=False,
        dag=dag
    )
    """
    return OpenLineageAdapter.build_task_instance_run_id(
        task.task_id, task_instance.execution_date, task_instance.try_number
    )


def lineage_parent_id(run_id: str, task: "BaseOperator", task_instance: "TaskInstance"):
    """
    Macro function which returns the generated job and run id for a given task. This
    can be used to forward the ids from a task to a child run so the job
    hierarchy is preserved. Child run can create ParentRunFacet from those ids.
    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ lineage_parent_id(run_id, task, task_instance) }}'], # macro invoked
        provide_context=False,
        dag=dag
    )
    """
    job_name = OpenLineageAdapter.build_task_instance_run_id(
        task.task_id, task_instance.execution_date, task_instance.try_number
    )
    return f"{_JOB_NAMESPACE}/{job_name}/{run_id}"
