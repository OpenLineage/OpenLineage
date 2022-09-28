# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
from typing import TYPE_CHECKING, Optional, Union

import attr
from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import (
    execute_in_thread,
    EventBuilder,
)

if TYPE_CHECKING:
    from airflow.models import TaskInstance, BaseOperator, MappedOperator
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

run_data_holder = ActiveRunManager()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, 'task'):
        log.warning(
            f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
        return

    log.debug("OpenLineage listener got notification about task instance start")
    dagrun = task_instance.dag_run
    dag = task_instance.task.dag

    def on_running():
        task_instance_copy = copy.deepcopy(task_instance)
        task_instance_copy.render_templates()
        task = task_instance_copy.task

        run_id = EventBuilder.start_task(
            adapter=adapter,
            extractor_manager=extractor_manager,
            task_instance=task_instance_copy,
            task=task,
            dag=dag,
            dagrun=dagrun,
        )
        run_data_holder.set_active_run(task_instance_copy, run_id)

    execute_in_thread(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance success")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    kwargs = dict(
        adapter=adapter,
        extractor_manager=extractor_manager,
        task_instance=task_instance,
        task=task,
        dagrun=dagrun,
        run_id=run_data.run_id if run_data else None,
    )

    execute_in_thread(EventBuilder.complete_task, kwargs=kwargs)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance failure")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    kwargs = dict(
        adapter=adapter,
        extractor_manager=extractor_manager,
        task_instance=task_instance,
        task=task,
        dagrun=dagrun,
        run_id=run_data.run_id if run_data else None,
    )

    execute_in_thread(EventBuilder.fail_task, kwargs=kwargs)
