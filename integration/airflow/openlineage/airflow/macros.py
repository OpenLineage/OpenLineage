# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from openlineage.airflow.adapter import _DAG_NAMESPACE, OpenLineageAdapter
from openlineage.airflow.utils import get_job_name

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance


def lineage_job_namespace():
    """
    Macro function which returns Airflow OpenLineage namespace.
    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ macros.OpenLineagePlugin.lineage_job_namespace() }}'],
        provide_context=False,
        dag=dag
    )
    """
    return _DAG_NAMESPACE


def lineage_job_name(task_instance: TaskInstance):
    """
    Macro function which returns Airflow task name in OpenLineage format (`<dag_id>.<task_id>`).
    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ macros.OpenLineagePlugin.lineage_job_name(task_instance) }}'],
        provide_context=False,
        dag=dag
    )
    """
    return get_job_name(task_instance)


def lineage_run_id(task_instance: TaskInstance):
    """
    Macro function which returns the generated runId (UUID) for a given Airflow task. This
    can be used to forward the run id from a task to a child run so the job
    hierarchy is preserved. Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ macros.OpenLineagePlugin.lineage_run_id(task_instance) }}'],
        provide_context=False,
        dag=dag
    )
    """
    return OpenLineageAdapter.build_task_instance_run_id(
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        try_number=task_instance.try_number,
        execution_date=task_instance.execution_date,
    )


def lineage_parent_id(task_instance: TaskInstance):
    """
    Macro function which returns the job name and runId for a given Airflow task. This
    can be used to forward the ids from a task to a child run so the job
    hierarchy is preserved. Child run can create ParentRunFacet from those ids.

    Result format: `<namespace>/<job_name>/<run_id>`.

    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ macros.OpenLineagePlugin.lineage_parent_id(task_instance) }}'],
        provide_context=False,
        dag=dag
    )
    """
    return "/".join(
        [
            lineage_job_namespace(),
            lineage_job_name(task_instance),
            lineage_run_id(task_instance),
        ]
    )
