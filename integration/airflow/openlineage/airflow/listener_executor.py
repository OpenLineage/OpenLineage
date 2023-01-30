# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import logging
import uuid
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import TYPE_CHECKING, Optional, Union

import attr
from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import (
    DagUtils,
    get_airflow_run_facet,
    get_custom_facets,
    get_dagrun_start_end,
    get_job_name,
    get_task_location,
    print_exception,
)

from airflow.listeners import hookimpl

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models import BaseOperator, DagRun, MappedOperator, TaskInstance


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


class ListenerPlugin:
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.executor: Optional[Executor] = None  # type: ignore
        self.run_data_holder = ActiveRunManager()
        self.extractor_manager = ExtractorManager()
        self.adapter = OpenLineageAdapter()

    @hookimpl
    def on_task_instance_running(
        self,
        previous_state,
        task_instance: "TaskInstance",
        session: "Session"
    ):
        if not hasattr(task_instance, 'task'):
            self.log.warning(
                f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
            return

        self.log.debug("OpenLineage listener got notification about task instance start")
        dagrun = task_instance.dag_run
        dag = task_instance.task.dag

        parent_run_id = self.adapter.build_dag_run_id(dag.dag_id, dagrun.run_id)
        run_id = str(uuid.uuid4())

        self.run_data_holder.set_active_run(task_instance, run_id)

        @print_exception
        def on_running():
            task = task_instance.task
            task_uuid = self.adapter.build_task_instance_run_id(
                task.task_id, task_instance.execution_date, task_instance.try_number
            )
            task_metadata = self.extractor_manager.extract_metadata(dagrun, task)

            start, end = get_dagrun_start_end(dagrun=dagrun, dag=dag)

            self.adapter.start_task(
                run_id=run_id,
                job_name=get_job_name(task),
                job_description=dag.description,
                event_time=DagUtils.get_start_time(task_instance.start_date),
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
                        dagrun, task, dagrun.external_trigger, task_instance
                    ),
                    **get_airflow_run_facet(dagrun, dag, task_instance, task, task_uuid)
                }
            )

        from airflow import settings
        try:
            settings.reconfigure_orm(disable_connection_pool=True)
        except Exception as e:
            print(e)

        self.executor.submit(on_running)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance: "TaskInstance", session):
        self.log.debug("OpenLineage listener got notification about task instance success")
        run_data = self.run_data_holder.get_active_run(task_instance)

        dagrun = task_instance.dag_run
        task = task_instance.task

        @print_exception
        def on_success():
            task_metadata = self.extractor_manager.extract_metadata(
                dagrun, task, complete=True, task_instance=task_instance
            )
            self.adapter.complete_task(
                run_id=run_data.run_id,
                job_name=get_job_name(task),
                end_time=DagUtils.to_iso_8601(task_instance.end_date),
                task=task_metadata,
            )

        self.executor.submit(on_success)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance: "TaskInstance", session):
        self.log.debug("OpenLineage listener got notification about task instance failure")
        run_data = self.run_data_holder.get_active_run(task_instance)

        dagrun = task_instance.dag_run
        task = run_data.task if run_data else None

        @print_exception
        def on_failure():
            task_metadata = self.extractor_manager.extract_metadata(
                dagrun, task, complete=True, task_instance=task_instance
            )

            self.adapter.fail_task(
                run_id=run_data.run_id,
                job_name=get_job_name(task),
                end_time=DagUtils.to_iso_8601(task_instance.end_date),
                task=task_metadata,
            )

        self.executor.submit(on_failure)

    @hookimpl
    def on_starting(self, component):
        self.log.debug(f"on_starting: {component.__class__.__name__}")
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="openlineage_")

    @hookimpl
    def before_stopping(self, component):
        self.log.debug(f"before_stopping: {component.__class__.__name__}")
        self.executor.shutdown(wait=True)

    @hookimpl
    def on_dag_run_running(self, dag_run: "DagRun", msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_running`")
            return
        start, end = get_dagrun_start_end(dag_run, dag_run.dag)
        self.executor.submit(
            self.adapter.dag_started,
            dag_run=dag_run,
            msg=msg,
            nominal_start_time=DagUtils.get_start_time(start),
            nominal_end_time=DagUtils.to_iso_8601(end),
        )

    @hookimpl
    def on_dag_run_success(self, dag_run: "DagRun", msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_success`")
            return
        self.executor.submit(self.adapter.dag_success, dag_run=dag_run, msg=msg)

    @hookimpl
    def on_dag_run_failed(self, dag_run: "DagRun", msg: str):
        if not self.executor:
            self.log.error("Executor have not started before `on_dag_run_failed`")
            return
        self.executor.submit(self.adapter.dag_failed, dag_run=dag_run, msg=msg)
