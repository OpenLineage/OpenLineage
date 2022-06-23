# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import time
import os
import copy
import uuid

from airflow.models import DAG as AIRFLOW_DAG
from airflow.utils.state import State

from openlineage.airflow.extractors.manager import ExtractorManager
from openlineage.airflow.macros import lineage_run_id, lineage_parent_id
from openlineage.airflow.utils import (
    JobIdMapping,
    DagUtils,
    get_custom_facets,
    new_lineage_run_id, get_task_location, openlineage_job_name
)

from openlineage.airflow.adapter import OpenLineageAdapter, _DAG_DEFAULT_NAMESPACE

_DAG_NAMESPACE = os.getenv('OPENLINEAGE_NAMESPACE', None)
if not _DAG_NAMESPACE:
    _DAG_NAMESPACE = os.getenv(
        'MARQUEZ_NAMESPACE', _DAG_DEFAULT_NAMESPACE
    )

_ADAPTER = OpenLineageAdapter()
extractor_manager = ExtractorManager()


def has_lineage_backend_setup():
    from airflow.configuration import conf
    return conf.get("lineage", "backend") == "openlineage.lineage_backend.OpenLineageBackend"


class DAG(AIRFLOW_DAG):
    def __init__(self, *args, **kwargs):
        self.log.info("openlineage-airflow dag starting")
        macros = {}
        if kwargs.__contains__("user_defined_macros"):
            macros = kwargs["user_defined_macros"]
        macros["lineage_run_id"] = lineage_run_id
        macros["lineage_parent_id"] = lineage_parent_id
        kwargs["user_defined_macros"] = macros
        if kwargs.__contains__("lineage_custom_extractors"):
            for operator, extractor in kwargs['lineage_custom_extractors'].items():
                extractor_manager.add_extractor(operator, extractor)
            del kwargs['lineage_custom_extractors']

        self.has_lineage_backend = has_lineage_backend_setup()
        super().__init__(*args, **kwargs)

    def add_task(self, task):
        super().add_task(task)

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
    def _register_dagrun(self, dagrun, is_external_trigger: bool, execution_date: str):
        self.log.debug(f"self.task_dict: {self.task_dict}")
        parent_run_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f'{self.dag_id}.{dagrun.run_id}'))
        # Register each task in the DAG
        for task_id, task in self.task_dict.items():
            t = self._now_ms()
            try:
                task_metadata = extractor_manager.extract_metadata(dagrun, task)

                job_name = openlineage_job_name(task.dag_id, task.task_id)
                run_id = new_lineage_run_id(dagrun.run_id, task_id)

                task_run_id = _ADAPTER.start_task(
                    run_id,
                    job_name,
                    self.description,
                    DagUtils.to_iso_8601(self._now_ms()),
                    self.dag_id,
                    parent_run_id,
                    get_task_location(task),
                    DagUtils.get_start_time(execution_date),
                    DagUtils.get_end_time(execution_date, self.following_schedule(execution_date)),
                    task_metadata,
                    {**task_metadata.run_facets, **get_custom_facets(task, is_external_trigger)}
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

        if has_lineage_backend_setup():
            self.log.info("lineage backend is set up; dag is skipping COMPLETE events")
            return super(DAG, self).handle_callback(*args, **kwargs)

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
            self._openlineage_job_name_from_task_instance(task_instance), dagrun.run_id, session)
        task_metadata = extractor_manager.extract_metadata(
            dagrun, task, complete=True, task_instance=task_instance
        )

        job_name = openlineage_job_name(self.dag_id, task.task_id)
        run_id = new_lineage_run_id(dagrun.run_id, task.task_id)

        if not task_run_id:
            parent_run_id = str(uuid.uuid3(uuid.NAMESPACE_URL, f'{self.dag_id}.{dagrun.run_id}'))
            task_run_id = _ADAPTER.start_task(
                run_id,
                job_name,
                self.description,
                DagUtils.to_iso_8601(task_instance.start_date),
                self.dag_id,
                parent_run_id,
                get_task_location(task),
                DagUtils.to_iso_8601(task_instance.start_date),
                DagUtils.to_iso_8601(task_instance.end_date),
                task_metadata,
                {**task_metadata.run_facets, **get_custom_facets(task, False)}
            )

            if not task_run_id:
                self.log.warning('Could not emit lineage')

        self.log.debug(f'Setting task state: {task_instance.state}'
                       f' for {task_instance.task_id}')
        if task_instance.state in {State.SUCCESS, State.SKIPPED}:
            _ADAPTER.complete_task(
                task_run_id,
                job_name,
                DagUtils.to_iso_8601(task_instance.end_date),
                task_metadata
            )
        else:
            _ADAPTER.fail_task(
                task_run_id,
                job_name,
                DagUtils.to_iso_8601(task_instance.end_date),
                task_metadata
            )

    def __deepcopy__(self, memo):
        """
        Override __deepcopy__ to avoid copying the _log property,
        which causes failure when pickling

        :param memo:
        :return:
        """
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'user_defined_filters', 'params', '_log'):
                try:
                    deepcopy = copy.deepcopy(v, memo)
                    setattr(result, k, deepcopy)
                except TypeError as e:
                    self.log.error(f"Unable to copy property{k}")
                    raise RuntimeError(f"Unable to copy property{k}") from e

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        result.params = self.params
        return result

    def _timed_log_message(self, start_time):
        return f'airflow_dag_id={self.dag_id} ' \
            f'duration_ms={(self._now_ms() - start_time)}'

    @staticmethod
    def _openlineage_job_name_from_task_instance(task_instance):
        return openlineage_job_name(task_instance.dag_id, task_instance.task_id)

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))
