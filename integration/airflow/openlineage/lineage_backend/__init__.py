# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import os
import time
from typing import Optional

from airflow.lineage.backend import LineageBackend
from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version


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
        from openlineage.airflow.utils import EventBuilder

        dag = context['dag']
        dagrun = context['dag_run']
        task_instance = context['task_instance']

        # this is going to be removed when removing 1.x support
        run_id = None
        if parse_version(AIRFLOW_VERSION) >= parse_version("2.0.0"):
            run_id = EventBuilder.start_task(
                adapter=self.adapter,
                extractor_manager=self.extractor_manager,
                task_instance=task_instance,
                task=operator,
                dag=dag,
                dagrun=dagrun,
            )

        EventBuilder.complete_task(
            adapter=self.adapter,
            extractor_manager=self.extractor_manager,
            task_instance=task_instance,
            task=operator,
            dagrun=dagrun,
            run_id=run_id,
            end_date=self._now_ms(),
        )

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))


class OpenLineageBackend(LineageBackend):
    # Airflow 1.10 uses send_lineage as staticmethod, so just construct class
    # instance on first use and delegate calls to it
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
