import logging
import pprint
import uuid
from typing import TYPE_CHECKING
from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import DagUtils, get_task_location, get_job_name, get_custom_facets

if TYPE_CHECKING:
    from airflow.models import TaskInstance


class RunIdHolder:
    def __init__(self):
        self.ids = {}

    def get_run_id(self, task_instance: "TaskInstance"):
        pk = task_instance.dag_id + task_instance.task_id + task_instance.run_id
        if pk not in self.ids:
            self.ids[pk] = uuid.uuid4()
        return self.ids[pk]


id_holder = RunIdHolder()
extractor_manager = ExtractorManager()
adapter = OpenLineageAdapter()


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    logging.getLogger('airflow').warning(pprint.pformat(task_instance.__dict__))
    ti_context = task_instance.get_template_context(session)
    dag = ti_context['dag']
    dagrun = ti_context['dag_run']
    task = ti_context['task']

    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

    adapter.start_task(
        run_id=id_holder.get_run_id(task_instance),
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


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    ti_context = task_instance.get_template_context(session)
    dagrun = ti_context['dag_run']
    task = ti_context['task']
    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

    adapter.complete_task(
        run_id=id_holder.get_run_id(task_instance),
        job_name=get_job_name(task),
        end_time=DagUtils.to_iso_8601(task_instance.end_date),
        task=task_metadata
    )


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    ti_context = task_instance.get_template_context(session)
    dagrun = ti_context['dag_run']
    task = ti_context['task']
    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

    adapter.fail_task(
        run_id=id_holder.get_run_id(task_instance),
        job_name=get_job_name(task),
        end_time=DagUtils.to_iso_8601(task_instance.end_date),
        task=task_metadata
    )
