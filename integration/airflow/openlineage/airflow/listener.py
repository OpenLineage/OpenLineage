import logging
import os
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
class RunData:
    run_id: str = attr.ib()
    task: "BaseOperator" = attr.ib()


class RunDataHolder:
    def __init__(self):
        self.run_data = {}

    def set_run_data(self, task_instance: "TaskInstance", run_id: str):
        self.run_data[self._pk(task_instance)] = RunData(run_id, task_instance.task)

    def get_run_data(self, task_instance: "TaskInstance") -> Optional[RunData]:
        pk = self._pk(task_instance)
        if pk not in self.run_data:
            return None
        return self.run_data[pk]

    def _pk(self, ti: "TaskInstance"):
        return ti.dag_id + ti.task_id + ti.run_id


run_data_holder = RunDataHolder()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()
log = logging.getLogger('airflow')


def run_thread(target: Callable, kwargs=None):
    if kwargs is None:
        kwargs = {}
    thread = threading.Thread(
        target=target,
        kwargs=kwargs,
        daemon=True
    )
    thread.start()
    # Join, but ignore checking if thread stopped. If it did, then we shoudn't do anything.
    # This basically gives this thread 2 seconds to complete work, then it can be killed,
    # as daemon=True. We don't want to deadlock this runner if our code hangs.
    thread.join(timeout=2)


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, 'task'):
        log.warning(f"No task set for TI object {id(task_instance)}")
        return

    dagrun = task_instance.dag_run
    task = task_instance.task
    dag = task_instance.task.dag

    run_id = str(uuid.uuid4())
    run_data_holder.set_run_data(task_instance, run_id)

    def on_running():
        task_metadata = extractor_manager.extract_metadata(dagrun, task, complete=False)

        adapter.start_task(
            run_id=run_id,
            job_name=get_job_name(task),
            job_description=dag.description,
            event_time=DagUtils.get_start_time(task_instance.start_date),
            parent_run_id=dagrun.run_id,
            code_location=get_task_location(task),
            nominal_start_time=DagUtils.get_start_time(dagrun.execution_date),
            nominal_end_time=DagUtils.to_iso_8601(task_instance.end_date),
            task=task_metadata,
            run_facets={
                **task_metadata.run_facets,
                **get_custom_facets(task_instance, dagrun.external_trigger)
            }
        )
    run_thread(on_running)


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    run_data = run_data_holder.get_run_data(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task

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
    run_thread(on_success)


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    run_data = run_data_holder.get_run_data(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task

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
    run_thread(on_failure)
