# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List, Union
from uuid import uuid4

import airflow.models
import time

from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import DagRun
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.state import State

from marquez_airflow.extractors import StepMetadata
from marquez_airflow.extractors.bigquery_extractor import BigQueryExtractor
from marquez_airflow.extractors.postgres_extractor import PostgresExtractor
from marquez_airflow.utils import (
    JobIdMapping,
    get_location,
    DagUtils,
    get_custom_facets
)

# Handling of import of different airflow versions
from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) >= parse_version("1.10.11"):
    from airflow import LoggingMixin
else:
    # Corrects path of import for Airflow versions below 1.10.11
    from airflow.utils.log.logging_mixin import LoggingMixin

from marquez_airflow.marquez import MarquezAdapter

_MARQUEZ = MarquezAdapter()

# TODO: Manually define operator->extractor mappings for now,
# but we'll want to encapsulate this logic in an 'Extractors' class
# with more convenient methods (ex: 'Extractors.extractor_for_task()')
_EXTRACTORS = {
    PostgresOperator: PostgresExtractor,
    BigQueryOperator: BigQueryExtractor
    # Append new extractors here
}


class DAG(airflow.models.DAG, LoggingMixin):
    def __init__(self, *args, **kwargs):
        self.log.debug("marquez-airflow dag starting")
        super().__init__(*args, **kwargs)

    def create_dagrun(self, *args, **kwargs):
        # run Airflow's create_dagrun() first
        dagrun = super(DAG, self).create_dagrun(*args, **kwargs)

        create_dag_start_ms = self._now_ms()
        try:
            self._register_dagrun(
                dagrun,
                kwargs.get('external_trigger', False),
                DagUtils.get_execution_date(**kwargs)
            )
        except Exception as e:
            self.log.error(
                f'Failed to record metadata: {e} '
                f'{self._timed_log_message(create_dag_start_ms)}',
                exc_info=True)

        return dagrun

    # We make the assumption that when a DAG run is created, its
    # tasks can be safely marked as started as well.
    # Doing it other way would require to hook up to
    # scheduler, where tasks are actually started
    def _register_dagrun(self, dagrun: DagRun, is_external_trigger: bool, execution_date: str):
        self.log.debug(f"self.task_dict: {self.task_dict}")
        # Register each task in the DAG
        for task_id, task in self.task_dict.items():
            t = self._now_ms()
            try:
                step = self._extract_metadata(dagrun, task)

                job_name = self._marquez_job_name(self.dag_id, task.task_id)
                run_id = self._marquez_run_id(dagrun.run_id, task.task_id)

                task_run_id = _MARQUEZ.start_task(
                    run_id,
                    job_name,
                    self.description,
                    DagUtils.to_iso_8601(self._now_ms()),
                    dagrun.run_id,
                    self._get_location(task),
                    DagUtils.get_start_time(execution_date),
                    DagUtils.get_end_time(execution_date, self.following_schedule(execution_date)),
                    step,
                    {**step.run_facets, **get_custom_facets(task, is_external_trigger)}
                )

                JobIdMapping.set(
                    job_name,
                    dagrun.run_id,
                    task_run_id
                )
            except Exception as e:
                self.log.error(
                    f'Failed to record task {task_id}: {e} '
                    f'{self._timed_log_message(t)}',
                    exc_info=True)

    def handle_callback(self, *args, **kwargs):
        self.log.debug(f"handle_callback({args}, {kwargs})")
        try:
            dagrun = args[0]
            self.log.debug(f"handle_callback() dagrun : {dagrun}")
            self._report_task_instances(
                dagrun,
                kwargs.get('session')
            )
        except Exception as e:
            self.log.error(
                f'Failed to record dagrun callback: {e} '
                f'dag_id={self.dag_id}',
                exc_info=True)

        return super().handle_callback(*args)

    def _report_task_instances(self, dagrun, session):
        task_instances = dagrun.get_task_instances()
        for task_instance in task_instances:
            try:
                self._report_task_instance(task_instance, dagrun, session)
            except Exception as e:
                self.log.error(
                    f'Failed to record task instance: {e} '
                    f'dag_id={self.dag_id}',
                    exc_info=True)

    def _report_task_instance(self, task_instance, dagrun, session):
        task = self.get_task(task_instance.task_id)

        # Note: task_run_id could be missing if it was removed from airflow
        # or the job could not be registered.
        task_run_id = JobIdMapping.pop(
            self._marquez_job_name_from_task_instance(task_instance), dagrun.run_id, session)
        step = self._extract_metadata(dagrun, task, task_instance)

        job_name = self._marquez_job_name(self.dag_id, task.task_id)
        run_id = self._marquez_run_id(dagrun.run_id, task.task_id)

        if not task_run_id:
            task_run_id = _MARQUEZ.start_task(
                run_id,
                job_name,
                self.description,
                DagUtils.to_iso_8601(task_instance.start_date),
                dagrun.run_id,
                self._get_location(task),
                DagUtils.to_iso_8601(task_instance.start_date),
                DagUtils.to_iso_8601(task_instance.end_date),
                step,
                {**step.run_facets, **get_custom_facets(task, False)}
            )

            if not task_run_id:
                self.log.warning('Could not emit lineage')

        self.log.debug(f'Setting task state: {task_instance.state}'
                       f' for {task_instance.task_id}')
        if task_instance.state in {State.SUCCESS, State.SKIPPED}:
            _MARQUEZ.complete_task(
                task_run_id,
                job_name,
                DagUtils.to_iso_8601(task_instance.end_date),
                step
            )
        else:
            _MARQUEZ.fail_task(
                task_run_id,
                job_name,
                DagUtils.to_iso_8601(task_instance.end_date),
                step
            )

    def _extract_metadata(self, dagrun, task, task_instance=None) -> StepMetadata:
        extractor = self._get_extractor(task)
        task_info = f'task_type={task.__class__.__name__} ' \
            f'airflow_dag_id={self.dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '
        if extractor:
            try:
                self.log.debug(
                    f'Using extractor {extractor.__name__} {task_info}')
                step = self._extract(extractor, task, task_instance)

                if isinstance(step, StepMetadata):
                    return step

                # Compatibility with custom extractors
                if isinstance(step, list):
                    if len(step) == 0:
                        return StepMetadata(
                            name=self._marquez_job_name(self.dag_id, task.task_id)
                        )
                    elif len(step) >= 1:
                        self.log.warning(
                            f'Extractor {extractor.__name__} {task_info} '
                            f'returned more then one StepMetadata instance: {step} '
                            f'will drop steps except for first!'
                        )
                    return step[0]

            except Exception as e:
                self.log.error(
                    f'Failed to extract metadata {e} {task_info}',
                    exc_info=True)
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')

        return StepMetadata(
            name=self._marquez_job_name(self.dag_id, task.task_id)
        )

    def _extract(self, extractor, task, task_instance) -> Union[StepMetadata, List[StepMetadata]]:
        if task_instance:
            step = extractor(task).extract_on_complete(task_instance)
            if step:
                return step

        return extractor(task).extract()

    def _get_extractor(self, task):
        extractor = _EXTRACTORS.get(task.__class__)
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        return extractor

    def _timed_log_message(self, start_time):
        return f'airflow_dag_id={self.dag_id} ' \
            f'duration_ms={(self._now_ms() - start_time)}'

    def new_run_id(self) -> str:
        return str(uuid4())

    @staticmethod
    def _get_location(task):
        try:
            if hasattr(task, 'file_path') and task.file_path:
                return get_location(task.file_path)
            else:
                return get_location(task.dag.fileloc)
        except Exception:
            return None

    @staticmethod
    def _marquez_job_name_from_task_instance(task_instance):
        return DAG._marquez_job_name(task_instance.dag_id, task_instance.task_id)

    @staticmethod
    def _marquez_job_name(dag_id: str, task_id: str) -> str:
        return f'{dag_id}.{task_id}'

    @staticmethod
    def _marquez_run_id(dag_run_id: str, task_id: str) -> str:
        return f'{dag_run_id}.{task_id}'

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))
