import logging
import pprint
import uuid
import attr

from typing import TYPE_CHECKING, Optional

from airflow.listeners import hookimpl

from openlineage.airflow.adapter import OpenLineageAdapter
from openlineage.airflow.extractors import ExtractorManager
from openlineage.airflow.utils import DagUtils, get_task_location, get_job_name, get_custom_facets

if TYPE_CHECKING:
    from airflow.models import TaskInstance, BaseOperator
    from sqlalchemy.orm import Session


@attr.s
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


@hookimpl
def on_task_instance_running(previous_state, task_instance: "TaskInstance", session: "Session"):
    if not hasattr(task_instance, 'task'):
        logging.getLogger('airflow').warning(f"No task set for TI object {id(task_instance)}")
    logging.getLogger('airflow').warning(pprint.pformat(task_instance.__dict__))
    logging.getLogger('airflow').warning(pprint.pformat(task_instance.dag_run.__dict__))
    logging.getLogger('airflow').warning(pprint.pformat(task_instance.task.__dict__))

    # ti_context = task_instance.get_template_context()
    dagrun = task_instance.dag_run
    task = task_instance.task
    dag = task_instance.task.dag
    # dag = task_instance.dag_run.dag
    # dagrun = task_instance.dag_run
    # task = task_instance.task

    run_id = str(uuid.uuid4())
    run_data_holder.set_run_data(task_instance, run_id)

    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

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


@hookimpl
def on_task_instance_success(previous_state, task_instance: "TaskInstance", session):
    logging.getLogger('airflow').warning(id(task_instance))
    logging.getLogger('airflow').warning(pprint.pformat(task_instance.__dict__))
    if 'dag_run' in task_instance.__dict__:
        logging.getLogger('airflow').warning(pprint.pformat(task_instance.dag_run.__dict__))
    if 'task' in task_instance.__dict__:
        logging.getLogger('airflow').warning(pprint.pformat(task_instance.task.__dict__))
        if 'dag' in task_instance.__dict__:
            logging.getLogger('airflow').warning(pprint.pformat(task_instance.task.dag.__dict__))

    run_data = run_data_holder.get_run_data(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task
    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

    adapter.complete_task(
        run_id=run_data.run_id,
        job_name=get_job_name(task),
        end_time=DagUtils.to_iso_8601(task_instance.end_date),
        task=task_metadata
    )


@hookimpl
def on_task_instance_failed(previous_state, task_instance: "TaskInstance", session):
    if 'dag_run' in task_instance.__dict__:
        logging.getLogger('airflow').warning(pprint.pformat(task_instance.dag_run.__dict__))
    if 'task' in task_instance.__dict__:
        logging.getLogger('airflow').warning(pprint.pformat(task_instance.task.__dict__))
        if 'dag' in task_instance.__dict__:
            logging.getLogger('airflow').warning(pprint.pformat(task_instance.task.dag.__dict__))

    run_data = run_data_holder.get_run_data(task_instance)

    dagrun = task_instance.dag_run
    task = run_data.task
    task_metadata = extractor_manager.extract_metadata(dagrun, task, task_instance)

    adapter.fail_task(
        run_id=run_data.run_id,
        job_name=get_job_name(task),
        end_time=DagUtils.to_iso_8601(task_instance.end_date),
        task=task_metadata
    )
