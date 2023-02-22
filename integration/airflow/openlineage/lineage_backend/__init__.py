# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time
import uuid
from typing import Optional

from pkg_resources import parse_version

from airflow.lineage.backend import LineageBackend
from airflow.version import version as AIRFLOW_VERSION


class Backend:
    def __init__(self):
        from openlineage.airflow.adapter import OpenLineageAdapter
        from openlineage.airflow.extractors.manager import ExtractorManager
        self.extractor_manager = ExtractorManager()
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
        from openlineage.airflow.adapter import OpenLineageAdapter
        from openlineage.airflow.utils import (
            DagUtils,
            get_airflow_run_facet,
            get_custom_facets,
            get_dagrun_start_end,
            get_job_name,
            get_task_location,
        )
        dag = context['dag']
        dagrun = context['dag_run']
        task_instance = context['task_instance']
        dag_run_id = self.adapter.build_dag_run_id(dag.dag_id, dagrun.run_id)

        run_id = str(uuid.uuid4())
        job_name = get_job_name(operator)

        task_metadata = self.extractor_manager.extract_metadata(
            dagrun=dagrun,
            task=operator,
            complete=True,
            task_instance=task_instance
        )

        task_uuid = OpenLineageAdapter.build_task_instance_run_id(
            operator.task_id, task_instance.execution_date, task_instance.try_number
        )
        start, end = get_dagrun_start_end(dagrun, dag)

        self.adapter.start_task(
            run_id=run_id,
            job_name=job_name,
            job_description=dag.description,
            event_time=DagUtils.get_start_time(task_instance.start_date),
            parent_job_name=dag.dag_id,
            parent_run_id=dag_run_id,
            code_location=get_task_location(operator),
            nominal_start_time=DagUtils.get_start_time(start),
            nominal_end_time=DagUtils.to_iso_8601(end),
            owners=dag.owner.split(', '),
            task=task_metadata,
            run_facets={
                **task_metadata.run_facets,
                **get_custom_facets(dagrun, operator, dagrun.external_trigger),
                **get_airflow_run_facet(dagrun, dag, task_instance, operator, task_uuid)
            }
        )

        self.adapter.complete_task(
            run_id=run_id,
            job_name=job_name,
            end_time=DagUtils.to_iso_8601(self._now_ms()),
            task=task_metadata,
        )

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))


class OpenLineageBackend(LineageBackend):
    # Constructing instance on first use and delegate calls to it - backwards compatible with 1.10
    backend: Optional[Backend] = None

    @classmethod
    def send_lineage(cls, *args, **kwargs):
        # Do not use LineageBackend approach when we can use plugins
        if parse_version(AIRFLOW_VERSION) >= parse_version("2.3.0.dev0"):
            return
        # Make this method a noop if OPENLINEAGE_DISABLED is set to true
        if os.getenv("OPENLINEAGE_DISABLED", None) in [True, 'true', "True"]:
            return
        if not cls.backend:
            cls.backend = Backend()
        return cls.backend.send_lineage(*args, **kwargs)
