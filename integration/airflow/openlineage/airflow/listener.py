import logging
import threading
import uuid

import attr

from typing import TYPE_CHECKING, Optional, Callable

from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import DagUtils, get_task_location, get_job_name, get_custom_facets

if TYPE_CHECKING:
    from airflow.models import TaskInstance, BaseOperator
    from sqlalchemy.orm import Session


@attr.s(frozen=True)
class ActiveRun:
    run_id: str = attr.ib()
    task: "BaseOperator" = attr.ib()


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


def execute_in_thread(target: Callable, kwargs=None):
    if kwargs is None:
        kwargs = {}
    thread = threading.Thread(
        target=target,
        kwargs=kwargs,
        daemon=True
    )
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


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, 'task'):
        log.warning(
            f"No task set for TI object task_id: {task_instance.task_id} - dag_id: {task_instance.dag_id} - run_id {task_instance.run_id}")  # noqa
        return

    log.debug("OpenLineage listener got notification about task instance start")
    dagrun = task_instance.dag_run
    task = task_instance.task
    dag = task_instance.task.dag

    run_id = str(uuid.uuid4())
    run_data_holder.set_active_run(task_instance, run_id)
    parent_run_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f'{dag.dag_id}.{dagrun.run_id}'))

    def on_running():
        task_metadata = extractor_manager.extract_metadata(dagrun, task)

        adapter.start_task(
            run_id=run_id,
            job_name=get_job_name(task),
            job_description=dag.description,
            event_time=DagUtils.get_start_time(task_instance.start_date),
            parent_job_name=dag.dag_id,
            parent_run_id=parent_run_id,
            code_location=get_task_location(task),
            nominal_start_time=DagUtils.get_start_time(dagrun.execution_date),
            nominal_end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
            run_facets={
                **task_metadata.run_facets,
                **get_custom_facets(task, dagrun.external_trigger)
            }
        )

    execute_in_thread(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance success")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    def on_success():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )
        adapter.complete_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata
        )

    execute_in_thread(on_success)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    log.debug("OpenLineage listener got notification about task instance failure")
    run_data = run_data_holder.get_active_run(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task if run_data else None

    def on_failure():
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )

        adapter.fail_task(
            run_id=run_data.run_id,
            job_name=get_job_name(task),
            end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata
        )

    execute_in_thread(on_failure)
