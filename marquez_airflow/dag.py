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

import logging
from uuid import uuid4

import airflow.models
import time
from airflow import LoggingMixin
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.state import State

from marquez_airflow.extractors import StepMetadata
from marquez_airflow.extractors.bigquery_extractor import BigQueryExtractor
from marquez_airflow.extractors.postgres_extractor import PostgresExtractor
from marquez_airflow.utils import (
    JobIdMapping,
    get_location,
    add_airflow_info_to,
    DagUtils
)

from marquez_airflow.marquez import Marquez

log = logging.getLogger(__name__)


class DAG(airflow.models.DAG, LoggingMixin):
    _job_id_mapping = None
    _marquez = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._job_id_mapping = JobIdMapping()
        self._marquez = Marquez()
        # TODO: Manually define operator->extractor mappings for now,
        # but we'll want to encapsulate this logic in an 'Extractors' class
        # with more convenient methods (ex: 'Extractors.extractor_for_task()')
        self._extractors = {
            PostgresOperator: PostgresExtractor,
            BigQueryOperator: BigQueryExtractor
            # Append new extractors here
        }
        self.log.debug(
            f"DAG successfully created with extractors: {self._extractors}"
        )

    def create_dagrun(self, *args, **kwargs):
        # run Airflow's create_dagrun() first
        dagrun = super(DAG, self).create_dagrun(*args, **kwargs)

        create_dag_start_ms = self._now_ms()
        try:
            self._marquez.create_namespace()
            self._register_dagrun(
                dagrun,
                DagUtils.get_execution_date(**kwargs),
                DagUtils.get_run_args(**kwargs)
            )
        except Exception as e:
            self.log.error(
                f'Failed to record metadata: {e} '
                f'{self._timed_log_message(create_dag_start_ms)}',
                exc_info=True)

        return dagrun

    def _register_dagrun(self, dagrun, execution_date, run_args):
        self.log.debug(f"self.task_dict: {self.task_dict}")
        # Register each task in the DAG
        for task_id, task in self.task_dict.items():
            t = self._now_ms()
            try:
                steps = self._extract_metadata(dagrun, task)
                [self._marquez.create_job(
                    step, self._get_location(task), self.description)
                    for step in steps]
                marquez_jobrun_ids = [self._marquez.create_run(
                    self.new_run_id(),
                    step,
                    run_args,
                    DagUtils.get_start_time(execution_date),
                    DagUtils.get_end_time(
                        execution_date,
                        self.following_schedule(execution_date))
                ) for step in steps]
                self._job_id_mapping.set(
                    self._marquez_job_name(self.dag_id, task.task_id),
                    dagrun.run_id,
                    marquez_jobrun_ids)
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
            self._marquez.create_namespace()
            self._report_task_instances(
                dagrun,
                DagUtils.get_run_args(**kwargs),
                kwargs.get('session')
            )
        except Exception as e:
            self.log.error(
                f'Failed to record dagrun callback: {e} '
                f'dag_id={self.dag_id}',
                exc_info=True)

        return super().handle_callback(*args)

    def _report_task_instances(self, dagrun, run_args, session):
        task_instances = dagrun.get_task_instances()
        for ti in task_instances:
            try:
                self._report_task_instance(ti, dagrun, run_args, session)
            except Exception as e:
                self.log.error(
                    f'Failed to record task instance: {e} '
                    f'dag_id={self.dag_id}',
                    exc_info=True)

    def _report_task_instance(self, ti, dagrun, run_args, session):
        task = self.get_task(ti.task_id)
        run_ids = self._job_id_mapping.pop(
            self._marquez_job_name_from_ti(ti), dagrun.run_id, session)
        steps = self._extract_metadata(dagrun, task, ti)

        # Note: run_ids could be missing if it was removed from airflow
        # or the job could not be registered.
        if not run_ids:
            [self._marquez.create_job(
                step, self._get_location(task), self.description)
             for step in steps]
            run_ids = [self._marquez.create_run(
                self.new_run_id(),
                step,
                run_args,
                DagUtils.to_iso_8601(ti.start_date),
                DagUtils.to_iso_8601(ti.end_date)
            ) for step in steps]
            if not run_ids:
                self.log.warn('Could not emit lineage')

        for step in steps:
            for run_id in run_ids:
                self._marquez.create_job(
                    step, self._get_location(task), self.description,
                    ti.state, run_id)
                self._marquez.start_run(
                    run_id,
                    DagUtils.to_iso_8601(ti.start_date))

                self.log.debug(f'Setting task state: {ti.state}'
                               f' for {ti.task_id}')
                if ti.state in {State.SUCCESS, State.SKIPPED}:
                    self._marquez.complete_run(
                        run_id,
                        DagUtils.to_iso_8601(ti.end_date))
                else:
                    self._marquez.fail_run(
                        run_id,
                        DagUtils.to_iso_8601(ti.end_date))

    def _extract_metadata(self, dagrun, task, ti=None):
        extractor = self._get_extractor(task)
        task_info = f'task_type={task.__class__.__name__} ' \
            f'airflow_dag_id={self.dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '
        if extractor:
            try:
                self.log.debug(
                    f'Using extractor {extractor.__name__} {task_info}')
                steps = self._extract(extractor, task, ti)

                return add_airflow_info_to(
                    task,
                    steps
                )
            except Exception as e:
                self.log.error(
                    f'Failed to extract metadata {e} {task_info}',
                    exc_info=True)
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')

        return add_airflow_info_to(
            task,
            [StepMetadata(name=self._marquez_job_name(
                self.dag_id, task.task_id))]
        )

    def _extract(self, extractor, task, ti):
        if ti:
            steps = extractor(task).extract_on_complete(ti)
            if steps:
                return steps

        return extractor(task).extract()

    def _get_extractor(self, task):
        extractor = self._extractors.get(task.__class__)
        log.debug(f'extractor for {task.__class__} is {extractor}')
        return extractor

    def _timed_log_message(self, start_time):
        return f'airflow_dag_id={self.dag_id} ' \
            f'duration_ms={(self._now_ms() - start_time)}'

    def new_run_id(self) -> str:
        return str(uuid4())

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))

    @staticmethod
    def _get_location(task):
        try:
            if hasattr(task, 'file_path') and task.file_path:
                return get_location(task.file_path)
            else:
                return get_location(task.dag.fileloc)
        except Exception:
            log.warning(f"Failed to get location for task '{task.task_id}'.",
                        exc_info=True)
            return None

    @staticmethod
    def _marquez_job_name_from_ti(ti):
        return DAG._marquez_job_name(ti.dag_id, ti.task_id)

    @staticmethod
    def _marquez_job_name(dag_id, task_id):
        return f'{dag_id}.{task_id}'
