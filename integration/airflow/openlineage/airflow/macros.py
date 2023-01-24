# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os

from openlineage.airflow.utils import JobIdMapping, openlineage_job_name

_JOB_NAMESPACE = os.getenv('OPENLINEAGE_NAMESPACE', 'default')


def lineage_run_id(run_id, task):
    """
    Macro function which returns the generated run id for a given task. This
    can be used to forward the run id from a task to a child run so the job
    hierarchy is preserved. Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ lineage_run_id(run_id, task) }}'], # lineage_run_id macro invoked
        provide_context=False,
        dag=dag
    )
    """
    from airflow.utils.session import create_session
    with create_session() as session:
        name = openlineage_job_name(task.dag_id, task.task_id)
        ids = JobIdMapping.get(name, run_id, session)
        if ids is None:
            return ""
        elif isinstance(ids, list):
            return "" if len(ids) == 0 else ids[0]
        else:
            return str(ids)


def lineage_parent_id(run_id, task):
    """
    Macro function which returns the generated job and run id for a given task. This
    can be used to forward the ids from a task to a child run so the job
    hierarchy is preserved. Child run can create ParentRunFacet from those ids.
    Invoke as a jinja template, e.g.

    PythonOperator(
        task_id='render_template',
        python_callable=my_task_function,
        op_args=['{{ lineage_parent_id(run_id, task) }}'], # lineage_run_id macro invoked
        provide_context=False,
        dag=dag
    )
    """
    from airflow.utils.session import create_session
    with create_session() as session:
        job_name = openlineage_job_name(task.dag_id, task.task_id)
        ids = JobIdMapping.get(job_name, run_id, session)
        if ids is None:
            return ""
        elif isinstance(ids, list):
            run_id = "" if len(ids) == 0 else ids[0]
        else:
            run_id = str(ids)
        return f"{_JOB_NAMESPACE}/{job_name}/{run_id}"
