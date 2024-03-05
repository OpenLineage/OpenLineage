# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import typing

from openlineage.airflow.adapter import _DAG_NAMESPACE, OpenLineageAdapter

if typing.TYPE_CHECKING:
    from airflow.models import TaskInstance


def lineage_run_id(task_instance: "TaskInstance"):
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
        task_instance.dag_id,
        task_instance.task.task_id,
        task_instance.execution_date,
        task_instance.try_number,
    )


def lineage_parent_id(task_instance: "TaskInstance"):
    """
    Macro function which returns the generated job and run id for a given task. This
    can be used to forward the ids from a task to a child run so the job
    hierarchy is preserved. Child run can create ParentRunFacet from those ids.
    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ lineage_parent_id(task, task_instance) }}'], # macro invoked
        provide_context=False,
        dag=dag
    )
    """
    return "/".join(
        [
            _DAG_NAMESPACE,
            f"{task_instance.dag_id}.{task_instance.task.task_id}",
            lineage_run_id(task_instance=task_instance),
        ]
    )
