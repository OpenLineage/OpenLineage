# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import threading
import uuid
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional, Callable, Union

import attr
from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import (
    DagUtils,
    get_dagrun_start_end,
    get_task_location,
    get_job_name,
    get_custom_facets,
    get_airflow_run_facet,
    is_airflow_version_enough,
    print_exception
)

if TYPE_CHECKING:
    from airflow.models import TaskInstance, BaseOperator, MappedOperator, DagRun
    from sqlalchemy.orm import Session


@attr.s(frozen=True)
class ActiveRun:
    run_id: str = attr.ib()
    task: Union["BaseOperator", "MappedOperator"] = attr.ib()


class ActiveRunManager:
    """Class that stores run data - run_id and task in-memory. This is needed because Airflow
    does not always pass all runtime info to on_task_instance_success and
    on_task_instance_failed that is needed to emit events. This is not a big problem since
    we're only running on worker - in separate process that is always spawned (or forked) on
    execution, just like old PHP runtime model.
    """

    def __init__(self):
        self.run_data = {}

    def set_active_run(self, task_instance: "TaskInstance", run_id: str):
        self.run_data[self._pk(task_instance)] = ActiveRun(run_id, task_instance.task)

    def get_active_run(self, task_instance: "TaskInstance") -> Optional[ActiveRun]:
        return self.run_data.get(self._pk(task_instance))

    @staticmethod
    def _pk(ti: "TaskInstance"):
        return ti.dag_id + ti.task_id + ti.run_id


log = logging.getLogger('airflow')
executor: Optional[Executor] = None  # type: ignore


def execute_in_thread(target: Callable, kwargs=None):
    if kwargs is None:
        kwargs = {}
    thread = threading.Thread(target=target, kwargs=kwargs, daemon=True)
    thread.start()

    # Join, but ignore checking if thread stopped. If it did, then we shoudn't do anything.
    # This basically gives this thread 5 seconds to complete work, then it can be killed,
    # as daemon=True. We don't want to deadlock Airflow if our code hangs.

    # This will hang if this timeouts, and extractor is running non-daemon thread inside,
    # since it will never be cleaned up. Ex. SnowflakeOperator
    thread.join(timeout=10)


run_data_holder = ActiveRunManager()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()


def execute(target: Callable):
    if is_airflow_version_enough("2.5.0"):
        executor.submit(target)  # type: ignore
    else:
        execute_in_thread(target)


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, 'task'):
        log.warning(
            f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
        return

    log.debug("OpenLineage listener got notification about task instance start")
    dagrun = task_instance.dag_run
    dag = task_instance.task.dag

    parent_run_id = adapter.build_dag_run_id(dag.dag_id, dagrun.run_id)
    run_id = str(uuid.uuid4())

    task_instance_copy = copy.deepcopy(task_instance)
    import os
    log.info(f"running {os.getpid()}")
    if is_airflow_version_enough("2.5.0"):
        # this needs to be set in 2.5 here to fetch in on_complete forked process
        run_data_holder.set_active_run(task_instance_copy, run_id)

    @print_exception
    def on_running():
        from airflow import settings
        try:
            settings.reconfigure_orm(disable_connection_pool=True)
        except Exception as e:
            print(e)
        log.info(f"hook on_running {os.getpid()}")
        task_instance_copy.render_templates()

        log.info(f"hook after render_templates {os.getpid()}")

        task = task_instance_copy.task
        task_uuid = adapter.build_task_instance_run_id(
            task.task_id, task_instance_copy.execution_date, task_instance_copy.try_number
        )
        if not is_airflow_version_enough("2.5.0"):
            # this needs to set here to pass proper task to on_complete
            run_data_holder.set_active_run(task_instance_copy, run_id)
        task_metadata = extractor_manager.extract_metadata(dagrun, task)

        start, end = get_dagrun_start_end(dagrun=dagrun, dag=dag)

        adapter.start_task(
            run_id=run_id,
            job_name=get_job_name(task),
            job_description=dag.description,
            event_time=DagUtils.get_start_time(task_instance_copy.start_date),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            code_location=get_task_location(task),
            nominal_start_time=DagUtils.get_start_time(start),
            nominal_end_time=DagUtils.to_iso_8601(end),
            owners=dag.owner.split(", "),
            task=task_metadata,
            run_facets={
                **task_metadata.run_facets,
                **get_custom_facets(
                    dagrun, task, dagrun.external_trigger, task_instance_copy
                ),
                **get_airflow_run_facet(dagrun, dag, task_instance_copy, task, task_uuid)
            }
        )
    
    from airflow import settings
    try:
        settings.reconfigure_orm(disable_connection_pool=True)
    except Exception as e:
        print(e)

    execute(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance success")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    if is_airflow_version_enough("2.5.0"):
        # in 2.5 task is set properly after flush
        task = task_instance.task
    else:
        task = run_data.task if run_data else None

    @print_exception
    def on_success():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )
        adapter.complete_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
        )

    execute(on_success)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance failure")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    @print_exception
    def on_failure():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )

        adapter.fail_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
        )

    execute(on_failure)


@hookimpl
def on_starting(component):
    global executor
    log.error(f"on_starting: {component.__class__.__name__}")
    if not executor:
        executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="openlineage_")


@hookimpl
def before_stopping(component):
    log.error(f"before_stopping: {component.__class__.__name__}")
    if executor:
        executor.shutdown(wait=True)


@hookimpl
def on_dag_run_running(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_running`")
        return
    start, end = get_dagrun_start_end(dag_run, dag_run.dag)
    executor.submit(
        adapter.dag_started,
        dag_run=dag_run,
        msg=msg,
        nominal_start_time=DagUtils.get_start_time(start),
        nominal_end_time=DagUtils.to_iso_8601(end),
    )


@hookimpl
def on_dag_run_success(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_success`")
        return
    executor.submit(adapter.dag_success, dag_run=dag_run, msg=msg)


@hookimpl
def on_dag_run_failed(dag_run: "DagRun", msg: str):
    if not executor:
        log.error("Executor have not started before `on_dag_run_failed`")
        return
    executor.submit(adapter.dag_failed, dag_run=dag_run, msg=msg)
