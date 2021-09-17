import logging
import os
from typing import Any, Dict, List, Optional, Type

from openlineage.client import OpenLineageClient
from openlineage.client import OpenLineageClientOptions
from openlineage.client import set_producer
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset
from openlineage.client.run import Job
from openlineage.client.run import Run
from openlineage.client.run import RunEvent
from openlineage.client.run import RunState


_DEFAULT_OWNER = "anonymous"
_DEFAULT_NAMESPACE = "default"

_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", None)
if not _NAMESPACE:
    _NAMESPACE = os.getenv("MARQUEZ_NAMESPACE", _DEFAULT_NAMESPACE)
OPENLINEAGE_PREFECT_VERSION = "0.0.0"

_PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/{OPENLINEAGE_PREFECT_VERSION}/integration/prefect"

set_producer(_PRODUCER)


log = logging.getLogger(__name__)


class OpenLineageAdapter:
    """
    Adapter for translating prefect events to OpenLineage events.
    """

    _client = None

    @property
    def client(self) -> OpenLineageClient:
        if not self._client:
            # Back comp with Marquez integration
            marquez_url = os.getenv("MARQUEZ_URL")
            marquez_api_key = os.getenv("MARQUEZ_API_KEY")
            if marquez_url:
                log.info(f"Sending lineage events to {marquez_url}")
                self._client = OpenLineageClient(
                    marquez_url, OpenLineageClientOptions(api_key=marquez_api_key, verify=False)
                )
            else:
                self._client = OpenLineageClient.from_environment()
        return self._client

    def ping(self):
        resp = self.client.session.get(self.client.url.replace("5000", "5001"))
        return resp.status_code == 200

    def start_task(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_run_id: Optional[str],
        code_location: Optional[str],
        inputs: Optional[Any],
        outputs: Optional[Any],
        run_facets: Optional[Dict[str, Type[BaseFacet]]] = None,  # Custom run facets
    ) -> str:
        """
        Emits openlineage event of type START
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task in dag
        :param job_description: user provided description of job
        :param event_time:
        :param parent_run_id: identifier of job spawning this task
        :param code_location: file path or URL of DAG file
        :param run_facets:
        :return:
        """
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(run_id, parent_run_id, job_name, run_facets),
            job=self._build_job(job_name, job_description, code_location),
            inputs=inputs or [],
            outputs=outputs or [],
            producer=_PRODUCER,
        )
        self.client.emit(event)
        return event.run.runId

    def complete_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        inputs: Optional[List[Dataset]],
        outputs: Optional[List[Dataset]],
    ):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param inputs: List of Input Datasets
        :param outputs: List of Output Datasets
        """
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(run_id),
            job=self._build_job(job_name),
            inputs=inputs,
            outputs=outputs,
            producer=_PRODUCER,
        )
        self.client.emit(event)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        inputs: Optional[Any],
        outputs: Optional[Any],
    ):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param inputs: List of Input Datasets
        :param outputs: List of Output Datasets
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(run_id),
            job=self._build_job(job_name),
            inputs=inputs,
            outputs=outputs,
            producer=_PRODUCER,
        )
        self.client.emit(event)

    @staticmethod
    def _build_run(
        run_id: str,
        parent_run_id: Optional[str] = None,
        job_name: Optional[str] = None,
        custom_facets: Dict[str, Type[BaseFacet]] = None,
    ) -> Run:
        facets = {}
        # if parent_run_id:
        #     facets.update({"parentRun": ParentRunFacet.create(parent_run_id, _NAMESPACE, job_name)})

        if custom_facets:
            facets.update(custom_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_description: Optional[str] = None,
        code_location: Optional[str] = None,
    ):
        facets = {}

        # if job_description:
        #     facets.update({"documentation": DocumentationJobFacet(job_description)})
        # if code_location:
        #     facets.update({"sourceCodeLocation": SourceCodeLocationJobFacet("", code_location)})

        return Job(_NAMESPACE, job_name, facets)
