# SPDX-License-Identifier: Apache-2.0

import logging
import uuid
import time
from pkg_resources import parse_version

from airflow.lineage.backend import LineageBackend
from airflow.version import version as AIRFLOW_VERSION


class Backend:
    def __init__(self):
        from openlineage.airflow.adapter import OpenLineageAdapter
        from openlineage.airflow.extractors.extractors import Extractors
        self.extractors = {}
        self.extractor_mapper = Extractors()
        self.log = logging.getLogger()
        self.adapter = OpenLineageAdapter()
    """
    Send OpenLineage events to lineage backend via airflow's LineageBackend mechanism.
    The start and complete events are send when task instance completes.
    """

    def send_lineage(
            self,
            operator=None,
            inlets=None,
            outlets=None,
            context=None
    ):
        """
        Send_lineage ignores manually provided inlets and outlets. The data collection mechanism
        is automatic, and bases on the passed context.
        """
        from openlineage.airflow.utils import DagUtils, get_custom_facets
        dag = context['dag']
        dagrun = context['dag_run']
        task_instance = context['task_instance']

        run_id = str(uuid.uuid4())
        job_name = self._openlineage_job_name(dag.dag_id, operator.task_id)

        task_metadata = self._extract_metadata(
            dag_id=dag.dag_id,
            dagrun=dagrun,
            task=operator,
            task_instance=task_instance
        )

        if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
            self.adapter.start_task(
                run_id=run_id,
                job_name=job_name,
                job_description=dag.description,
                event_time=DagUtils.get_start_time(task_instance.start_date),
                parent_run_id=dagrun.run_id,
                code_location=self._get_location(operator),
                nominal_start_time=DagUtils.get_start_time(dagrun.execution_date),
                nominal_end_time=DagUtils.to_iso_8601(task_instance.end_date),
                task=task_metadata,
                run_facets={
                    **task_metadata.run_facets,
                    **get_custom_facets(operator, dagrun.external_trigger)
                }
            )

        self.adapter.complete_task(
            run_id=run_id,
            job_name=job_name,
            end_time=DagUtils.to_iso_8601(self._now_ms()),
            task=task_metadata,
        )

    def _extract_metadata(self, dag_id, dagrun, task, task_instance=None):
        extractor = self._get_extractor(task)
        task_info = f'task_type={task.__class__.__name__} ' \
            f'airflow_dag_id={dag_id} ' \
            f'task_id={task.task_id} ' \
            f'airflow_run_id={dagrun.run_id} '
        if extractor:
            try:
                self.log.debug(
                    f'Using extractor {extractor.__class__.__name__} {task_info}')
                task_metadata = self._extract(extractor, task_instance)
                self.log.debug(
                    f"Found task metadata for operation {task.task_id}: {task_metadata}"
                )
                if task_metadata:
                    return task_metadata

            except Exception as e:
                self.log.exception(
                    f'Failed to extract metadata {e} {task_info}',
                )
        else:
            self.log.warning(
                f'Unable to find an extractor. {task_info}')
        from openlineage.airflow.extractors.base import TaskMetadata
        from openlineage.airflow.facets import UnknownOperatorAttributeRunFacet, \
            UnknownOperatorInstance

        return TaskMetadata(
            name=self._openlineage_job_name(dag_id, task.task_id),
            run_facets={
                "unknownSourceAttribute": UnknownOperatorAttributeRunFacet(
                    unknownItems=[
                        UnknownOperatorInstance(
                            name=task.__class__.__name__,
                            properties=self.operator
                        )
                    ]
                )
            }
        )

    def _extract(self, extractor, task_instance):
        if task_instance:
            task_metadata = extractor.extract_on_complete(task_instance)
            if task_metadata:
                return task_metadata

        return extractor.extract()

    def _get_extractor(self, task):
        if task.task_id in self.extractors:
            return self.extractors[task.task_id]
        extractor = self.extractor_mapper.get_extractor_class(task.__class__)
        self.log.debug(f'extractor for {task.__class__} is {extractor}')
        if extractor:
            self.extractors[task.task_id] = extractor(task)
            return self.extractors[task.task_id]
        return None

    @classmethod
    def _openlineage_job_name_from_task_instance(cls, task_instance):
        return cls._openlineage_job_name(task_instance.dag_id, task_instance.task_id)

    @staticmethod
    def _openlineage_job_name(dag_id: str, task_id: str) -> str:
        return f'{dag_id}.{task_id}'

    @staticmethod
    def _get_location(task):
        from openlineage.airflow.utils import get_location
        try:
            if hasattr(task, 'file_path') and task.file_path:
                return get_location(task.file_path)
            else:
                return get_location(task.dag.fileloc)
        except Exception:
            return None

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))


class OpenLineageBackend(LineageBackend):
    # Airflow 1.10 uses send_lineage as staticmethod, so just construct class
    # instance on first use and delegate calls to it
    backend: Backend = None

    @classmethod
    def send_lineage(cls, *args, **kwargs):
        if not cls.backend:
            cls.backend = Backend()
        return cls.backend.send_lineage(*args, **kwargs)
