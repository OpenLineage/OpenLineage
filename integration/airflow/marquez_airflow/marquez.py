import os
import logging
from typing import Optional, Dict, Type

from marquez_airflow import __version__ as MARQUEZ_AIRFLOW_VERSION
from marquez_airflow.extractors import StepMetadata

from openlineage.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.facet import DocumentationJobFacet, SourceCodeLocationJobFacet, SqlJobFacet, \
    NominalTimeRunFacet, ParentRunFacet, BaseFacet
from openlineage.run import RunEvent, RunState, Run, Job

_DAG_DEFAULT_OWNER = 'anonymous'
_DAG_DEFAULT_NAMESPACE = 'default'

_DAG_NAMESPACE = os.getenv(
    'MARQUEZ_NAMESPACE', _DAG_DEFAULT_NAMESPACE
)

log = logging.getLogger(__name__)


class MarquezAdapter:
    """
    Adapter for translating Airflow metadata to OpenLineage events,
    instead of directly creating them from Airflow code.
    """
    _client = None

    def get_or_create_openlineage_client(self) -> OpenLineageClient:
        if not self._client:
            marquez_url = os.getenv('MARQUEZ_URL')
            marquez_api_key = os.getenv('MARQUEZ_API_KEY')
            if marquez_url:
                self._client = OpenLineageClient(marquez_url, OpenLineageClientOptions(
                    api_key=marquez_api_key
                ))
            else:
                self._client = OpenLineageClient.from_environment()
        return self._client

    def start_task(
            self,
            run_id: str,
            job_name: str,
            job_description: str,
            event_time: str,
            parent_run_id: Optional[str],
            code_location: Optional[str],
            nominal_start_time: str,
            nominal_end_time: str,
            step: Optional[StepMetadata],
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
        :param nominal_start_time: scheduled time of dag run
        :param nominal_end_time: following schedule of dag run
        :param step: metadata container with information extracted from operator
        :param run_facets:
        :return:
        """
        sql = None
        if step:
            sql = step.context.get('sql', None)

        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(
                run_id, parent_run_id, job_name, nominal_start_time, nominal_end_time, run_facets
            ),
            job=self._build_job(
                job_name, job_description, code_location, sql
            ),
            producer=f"marquez-airflow/{MARQUEZ_AIRFLOW_VERSION}",
            inputs=[
                dataset.to_openlineage_dataset() for dataset in step.inputs
            ] if step else None,
            outputs=[
                dataset.to_openlineage_dataset() for dataset in step.outputs
            ] if step else None
        )
        self.get_or_create_openlineage_client().emit(event)
        return event.run.runId

    def complete_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        step: StepMetadata
    ):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param step: metadata container with information extracted from operator
        """
        sql = None
        if step:
            sql = step.context.get('sql', None)

        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(
                run_id
            ),
            job=self._build_job(
                job_name, sql=sql
            ),
            inputs=[
                dataset.to_openlineage_dataset() for dataset in step.inputs
            ],
            outputs=[
                dataset.to_openlineage_dataset() for dataset in step.outputs
            ],
            producer=f"marquez-airflow/{MARQUEZ_AIRFLOW_VERSION}"
        )
        self.get_or_create_openlineage_client().emit(event)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        step: StepMetadata
    ):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param step: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(
                run_id
            ),
            job=self._build_job(
                job_name
            ),
            inputs=[
                dataset.to_openlineage_dataset() for dataset in step.inputs
            ],
            outputs=[
                dataset.to_openlineage_dataset() for dataset in step.outputs
            ],
            producer=f"marquez-airflow/{MARQUEZ_AIRFLOW_VERSION}"
        )
        self.get_or_create_openlineage_client().emit(event)

    @staticmethod
    def _build_run(
            run_id: str,
            parent_run_id: Optional[str] = None,
            job_name: Optional[str] = None,
            nominal_start_time: Optional[str] = None,
            nominal_end_time: Optional[str] = None,
            custom_facets: Dict[str, Type[BaseFacet]] = None
    ) -> Run:
        facets = {}
        if nominal_start_time:
            facets.update({
                "nominalTime": NominalTimeRunFacet(nominal_start_time, nominal_end_time)
            })
        if parent_run_id:
            facets.update({"parentRun": ParentRunFacet.create(
                parent_run_id,
                _DAG_NAMESPACE,
                job_name
            )})

        if custom_facets:
            facets.update(custom_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
            job_name: str,
            job_description: Optional[str] = None,
            code_location: Optional[str] = None,
            sql: Optional[str] = None
    ):
        facets = {}

        if job_description:
            facets.update({
                "documentation": DocumentationJobFacet(job_description)
            })
        if code_location:
            facets.update({
                "sourceCodeLocation": SourceCodeLocationJobFacet("", code_location)
            })
        if sql:
            facets.update({
                "sql": SqlJobFacet(sql)
            })

        return Job(_DAG_NAMESPACE, job_name, facets)
