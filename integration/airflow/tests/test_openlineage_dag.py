# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import datetime
import logging
import uuid
from typing import List
from uuid import UUID

import mock
import openlineage.airflow.dag
import pytest
from pkg_resources import parse_version
from airflow.version import version as AIRFLOW_VERSION
if parse_version(AIRFLOW_VERSION) > parse_version("2.0.0"):
    pytestmark = pytest.mark.skip("Skipping tests for Airflow 2.0.0+")
from airflow.models import (TaskInstance, DagRun)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from openlineage.common.dataset import Source, Dataset
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import DbTableMeta
from openlineage.airflow import DAG
from openlineage.airflow import __version__ as OPENLINEAGE_AIRFLOW_VERSION
from openlineage.airflow.extractors import (
    BaseExtractor, TaskMetadata
)
from openlineage.airflow.facets import AirflowRunArgsRunFacet, \
    AirflowVersionRunFacet, UnknownOperatorAttributeRunFacet, UnknownOperatorInstance
from openlineage.airflow.utils import get_location, get_job_name, new_lineage_run_id
from openlineage.client.facet import NominalTimeRunFacet, SourceCodeLocationJobFacet, \
    DocumentationJobFacet, DataSourceDatasetFacet, SchemaDatasetFacet, \
    SchemaField, ParentRunFacet, set_producer
from openlineage.client.run import RunEvent, RunState, Job, Run, \
    Dataset as OpenLineageDataset

log = logging.getLogger(__name__)

NO_INPUTS = []
NO_OUTPUTS = []

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
DEFAULT_END_DATE = timezone.datetime(2016, 1, 2)

DAG_ID = 'test_dag'
DAG_RUN_ID = 'test_run_id_for_task_completed_and_failed'
DAG_RUN_ARGS = {'external_trigger': False}
# TODO: check with a different namespace and owner
DAG_NAMESPACE = 'default'
DAG_OWNER = 'anonymous'
DAG_DESCRIPTION = \
    'A simple DAG to test the openlineage.DAG metadata extraction flow.'

DAG_DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['owner@test.com']
}

TASK_ID_COMPLETED = 'test_task_completed'
TASK_ID_FAILED = 'test_task_failed'


PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/" \
    f"{OPENLINEAGE_AIRFLOW_VERSION}/integration/airflow"


@pytest.fixture(scope='session', autouse=True)
def setup_producer():
    set_producer(PRODUCER)


@pytest.fixture
@provide_session
def clear_db_airflow_dags(session=None):
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()


@provide_session
def test_new_lineage_run_id(clear_db_airflow_dags, session=None):
    run_id = new_lineage_run_id("dag_id", "task_id")
    assert UUID(run_id).version == 4


# tests a simple workflow with default extraction mechanism
@mock.patch('openlineage.airflow.dag.new_lineage_run_id')
@mock.patch('openlineage.airflow.dag.get_custom_facets')
@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
@mock.patch('openlineage.airflow.dag.JobIdMapping')
@provide_session
def test_openlineage_dag(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None
):
    dag = DAG(
        DAG_ID,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )
    # (1) Mock the openlineage client method calls
    mock_ol_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_ol_client

    run_id_completed = str(uuid.uuid4())
    run_id_failed = str(uuid.uuid4())

    job_id_completed = f"{DAG_ID}.{TASK_ID_COMPLETED}"
    job_id_failed = f"{DAG_ID}.{TASK_ID_FAILED}"

    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = [
        run_id_completed, run_id_failed, run_id_completed, run_id_failed
    ]

    # (2) Add task that will be marked as completed
    task_will_complete = DummyOperator(
        task_id=TASK_ID_COMPLETED,
        dag=dag
    )
    completed_task_location = get_location(task_will_complete.dag.fileloc)

    # (3) Add task that will be marked as failed
    task_will_fail = DummyOperator(
        task_id=TASK_ID_FAILED,
        dag=dag
    )
    failed_task_location = get_location(task_will_complete.dag.fileloc)

    # (4) Create DAG run and mark as running
    dagrun = dag.create_dagrun(
        run_id=DAG_RUN_ID,
        execution_date=DEFAULT_DATE,
        state=State.RUNNING)

    # Assert emit calls
    start_time = '2016-01-01T00:00:00.000000Z'
    end_time = '2016-01-02T00:00:00.000000Z'

    parent_run_id = make_parent_run_id()
    emit_calls = [
        mock.call(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id_completed, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=parent_run_id,
                    namespace=DAG_NAMESPACE,
                    name=DAG_ID
                ),
                'unknownSourceAttribute': UnknownOperatorAttributeRunFacet(
                    unknownItems=[UnknownOperatorInstance(name='DummyOperator',
                                                          properties=mock.ANY)])
            }),
            job=Job("default", job_id_completed, {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )),
        mock.call(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id_failed, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=parent_run_id,
                    namespace=DAG_NAMESPACE,
                    name=DAG_ID
                ),
                'unknownSourceAttribute': UnknownOperatorAttributeRunFacet(
                    unknownItems=[UnknownOperatorInstance(name='DummyOperator',
                                                          properties=mock.ANY)])
            }),
            job=Job("default", job_id_failed, {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", failed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ]
    log.info(
        f"{ [name for name, args, kwargs in mock_ol_client.mock_calls]}")
    mock_ol_client.emit.assert_has_calls(emit_calls)

    # (5) Start task that will be marked as completed
    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    # (6) Start task that will be marked as failed
    ti1 = TaskInstance(task=task_will_fail, execution_date=DEFAULT_DATE)
    ti1.state = State.FAILED
    session.add(ti1)
    session.commit()

    job_id_mapping.pop.side_effect = [run_id_completed, run_id_failed]

    dag.handle_callback(dagrun, success=False, session=session)

    emit_calls += [
        mock.call(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=mock.ANY,
            run=Run(run_id_completed, {
                'unknownSourceAttribute': UnknownOperatorAttributeRunFacet(
                    unknownItems=[UnknownOperatorInstance(name='DummyOperator',
                                                          properties=mock.ANY)])
            }),
            job=Job("default", job_id_completed),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )),
        mock.call(RunEvent(
            eventType=RunState.FAIL,
            eventTime=mock.ANY,
            run=Run(run_id_failed, {
                'unknownSourceAttribute': UnknownOperatorAttributeRunFacet(
                    unknownItems=[UnknownOperatorInstance(name='DummyOperator',
                                                          properties=mock.ANY)])
            }),
            job=Job("default", job_id_failed),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ]
    mock_ol_client.emit.assert_has_calls(emit_calls)


@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
@provide_session
def test_lineage_run_id(mock_get_or_create_openlineage_client, session=None):
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client

    dag = DAG(
        "test_lineage_run_id",
        schedule_interval="@daily",
        default_args=DAG_DEFAULT_ARGS,
        description="test dag"
    )

    class Collector:
        def update_task_id(self, tid):
            self.run_id = tid
            print(f"Got run id {self.run_id}")

    collector = Collector()
    t1 = PythonOperator(
        task_id='show_template',
        python_callable=collector.update_task_id,
        op_args=['{{ lineage_run_id(run_id, task) }}'],
        provide_context=False,
        dag=dag
    )

    dag.clear()
    today = datetime.datetime.now()
    dagrun = dag.create_dagrun(
        run_id="test_dag_run",
        execution_date=timezone.datetime(today.year, month=today.month, day=today.day),
        state=State.RUNNING,
        session=session)
    ti = dagrun.get_task_instance(t1.task_id)
    ti.task = t1
    ti.run()
    assert collector.run_id != ""


@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
@provide_session
def test_lineage_parent_id(mock_get_or_create_openlineage_client, session=None):
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client

    dag = DAG(
        "test_lineage_parent_id",
        schedule_interval="@daily",
        default_args=DAG_DEFAULT_ARGS,
        description="test dag"
    )

    class Collector:
        def update_task_id(self, tid):
            self.namespace, self.job_id, self.run_id = tid.split('/')
            print(f"Got namespace {self.namespace} - job id {self.job_id} - run id {self.run_id}")

    collector = Collector()
    t1 = PythonOperator(
        task_id='show_template',
        python_callable=collector.update_task_id,
        op_args=['{{ lineage_parent_id(run_id, task) }}'],
        provide_context=False,
        dag=dag
    )

    dag.clear()
    today = datetime.datetime.now()
    dagrun = dag.create_dagrun(
        run_id="test_dag_run",
        execution_date=timezone.datetime(today.year, month=today.month, day=today.day),
        state=State.RUNNING,
        session=session)
    ti = dagrun.get_task_instance(t1.task_id)
    ti.task = t1
    ti.run()
    assert collector.namespace != ""
    assert collector.job_id != ""
    assert collector.run_id != ""


class TestFixtureDummyOperator(DummyOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestFixtureDummyOperator, self).__init__(*args, **kwargs)


class TestFixtureDummyExtractor(BaseExtractor):
    source = Source(
        scheme="dummy",
        authority="localhost:1234",
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['TestFixtureDummyOperator']

    def extract(self) -> TaskMetadata:
        inputs = [
            Dataset.from_table(self.source, "extract_input1").to_openlineage_dataset()
        ]
        outputs = [
            Dataset.from_table(self.source, "extract_output1").to_openlineage_dataset()
        ]
        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs
        )

    def extract_on_complete(self, task_instance) -> TaskMetadata:
        return None


class TestFixtureDummyExtractorOnComplete(BaseExtractor):
    source = Source(
        scheme="dummy",
        authority="localhost:1234",
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['TestFixtureDummyOperator']

    def extract(self) -> TaskMetadata:
        return None

    def extract_on_complete(self, task_instance) -> TaskMetadata:
        inputs = [
            Dataset.from_table_schema(self.source, DbTableSchema(
                schema_name='schema',
                table_name=DbTableMeta('extract_on_complete_input1'),
                columns=[DbColumn(
                    name='field1',
                    type='text',
                    description='',
                    ordinal_position=1
                ),
                    DbColumn(
                    name='field2',
                    type='text',
                    description='',
                    ordinal_position=2
                )]
            )).to_openlineage_dataset()
        ]
        outputs = [
            Dataset.from_table(self.source, "extract_on_complete_output1").to_openlineage_dataset()
        ]
        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs,
        )


# test the lifecycle including with extractors
@mock.patch('openlineage.airflow.dag.new_lineage_run_id')
@mock.patch('openlineage.airflow.dag.get_custom_facets')
@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
@mock.patch('openlineage.airflow.dag.JobIdMapping')
@provide_session
def test_openlineage_dag_with_extractor(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    openlineage.airflow.dag.extractor_manager.\
        task_to_extractor.extractors[TestFixtureDummyOperator.__name__] = \
        TestFixtureDummyExtractor

    dag_id = 'test_openlineage_dag_with_extractor'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )

    dag_run_id = 'test_openlineage_dag_with_extractor_run_id'

    run_id = str(uuid.uuid4())
    job_id = f"{dag_id}.{TASK_ID_COMPLETED}"
    # Mock the openlineage client method calls
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.return_value = run_id

    # Add task that will be marked as completed
    task_will_complete = TestFixtureDummyOperator(
        task_id=TASK_ID_COMPLETED,
        dag=dag
    )
    completed_task_location = get_location(task_will_complete.dag.fileloc)

    # --- pretend run the DAG

    # Create DAG run and mark as running
    dagrun = dag.create_dagrun(
        run_id=dag_run_id,
        execution_date=DEFAULT_DATE,
        state=State.RUNNING)

    # --- Asserts that the job starting triggers openlineage event

    start_time = '2016-01-01T00:00:00.000000Z'
    end_time = '2016-01-02T00:00:00.000000Z'
    parent_run_id = make_parent_run_id(dag_id, dag_run_id)
    mock_openlineage_client.emit.assert_called_once_with(
        RunEvent(
            RunState.START,
            mock.ANY,
            Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=parent_run_id,
                    namespace=DAG_NAMESPACE,
                    name=dag_id
                )
            }),
            Job("default", job_id, {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            PRODUCER,
            [OpenLineageDataset('dummy://localhost:1234', 'extract_input1', {
                "dataSource": DataSourceDatasetFacet(
                    name='dummy://localhost:1234',
                    uri='dummy://localhost:1234?query_tag=asdf'
                )
            })],
            [OpenLineageDataset('dummy://localhost:1234', 'extract_output1', {
                "dataSource": DataSourceDatasetFacet(
                    name='dummy://localhost:1234',
                    uri='dummy://localhost:1234?query_tag=asdf'
                )
            })]
        )
    )

    mock_openlineage_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    # --- Assert that the openlineage call is done

    mock_openlineage_client.emit.assert_called_once_with(
        RunEvent(
            RunState.COMPLETE,
            mock.ANY,
            Run(run_id),
            Job("default", job_id),
            PRODUCER,
            [],
            []
        )
    )


@mock.patch('openlineage.airflow.dag.new_lineage_run_id')
@mock.patch('openlineage.airflow.dag.get_custom_facets')
@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
@mock.patch('openlineage.airflow.dag.JobIdMapping')
@provide_session
def test_openlineage_dag_with_extract_on_complete(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    openlineage.airflow.dag.extractor_manager.extractors.clear()
    openlineage.airflow.dag.extractor_manager.task_to_extractor.\
        extractors[TestFixtureDummyOperator.__name__] = \
        TestFixtureDummyExtractorOnComplete

    dag_id = 'test_openlineage_dag_with_extractor_on_complete'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )

    dag_run_id = 'test_openlineage_dag_with_extractor_run_id'

    run_id = str(uuid.uuid4())
    job_id = f"{dag_id}.{TASK_ID_COMPLETED}"
    # Mock the openlineage client method calls
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.return_value = run_id

    # Add task that will be marked as completed
    task_will_complete = TestFixtureDummyOperator(
        task_id=TASK_ID_COMPLETED,
        dag=dag
    )
    completed_task_location = get_location(task_will_complete.dag.fileloc)

    # Create DAG run and mark as running
    dagrun = dag.create_dagrun(
        run_id=dag_run_id,
        execution_date=DEFAULT_DATE,
        state=State.RUNNING)

    start_time = '2016-01-01T00:00:00.000000Z'
    end_time = '2016-01-02T00:00:00.000000Z'
    parent_run_id = make_parent_run_id(dag_id, dag_run_id)
    mock_openlineage_client.emit.assert_has_calls([
        mock.call(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=parent_run_id,
                    namespace=DAG_NAMESPACE,
                    name=dag_id
                )
            }),
            job=Job("default", job_id, {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ])

    mock_openlineage_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    mock_openlineage_client.emit.assert_has_calls([
        mock.call(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=mock.ANY,
            run=Run(run_id),
            job=Job("default", job_id),
            producer=PRODUCER,
            inputs=[OpenLineageDataset(
                namespace='dummy://localhost:1234',
                name='schema.extract_on_complete_input1',
                facets={
                    'dataSource': DataSourceDatasetFacet(
                        name='dummy://localhost:1234',
                        uri='dummy://localhost:1234?query_tag=asdf'
                    ),
                    'schema': SchemaDatasetFacet(
                        fields=[
                            SchemaField(name='field1', type='text', description=''),
                            SchemaField(name='field2', type='text', description='')
                        ]
                    )
                })
            ],
            outputs=[OpenLineageDataset(
                namespace='dummy://localhost:1234',
                name='extract_on_complete_output1',
                facets={
                    'dataSource': DataSourceDatasetFacet(
                        name='dummy://localhost:1234',
                        uri='dummy://localhost:1234?query_tag=asdf'
                    )
                })
            ]
        ))
    ])


# tests a simple workflow with default custom facet mechanism
@mock.patch('openlineage.airflow.dag.new_lineage_run_id')
@mock.patch('openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client')
def test_openlineage_dag_adds_custom_facets(
        mock_get_or_create_openlineage_client,
        new_lineage_run_id,
        clear_db_airflow_dags,
):
    openlineage.airflow.dag.extractor_manager.extractors.clear()
    openlineage.airflow.dag.extractor_manager.\
        task_to_extractor.extractors.pop('TestFixtureDummyOperator', None)

    dag = DAG(
        DAG_ID,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )
    # Mock the openlineage client method calls
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client

    run_id = str(uuid.uuid4())
    job_id = f"{DAG_ID}.{TASK_ID_COMPLETED}"

    new_lineage_run_id.return_value = run_id

    # Add task that will be marked as completed
    task_will_complete = DummyOperator(
        task_id=TASK_ID_COMPLETED,
        dag=dag
    )
    completed_task_location = get_location(task_will_complete.dag.fileloc)

    # Start run
    dag.create_dagrun(
        run_id=DAG_RUN_ID,
        execution_date=DEFAULT_DATE,
        state=State.RUNNING)

    # Assert emit calls
    start_time = '2016-01-01T00:00:00.000000Z'
    end_time = '2016-01-02T00:00:00.000000Z'
    parent_run_id = make_parent_run_id()
    mock_openlineage_client.emit.assert_called_once_with(RunEvent(
        eventType=RunState.START,
        eventTime=mock.ANY,
        run=Run(run_id, {
            "nominalTime": NominalTimeRunFacet(start_time, end_time),
            "parentRun": ParentRunFacet.create(
                runId=parent_run_id,
                namespace=DAG_NAMESPACE,
                name=DAG_ID
            ),
            "airflow_runArgs": AirflowRunArgsRunFacet(False),
            "airflow_version": AirflowVersionRunFacet(
                operator="airflow.operators.dummy_operator.DummyOperator",
                taskInfo=mock.ANY,
                airflowVersion=AIRFLOW_VERSION,
                openlineageAirflowVersion=OPENLINEAGE_AIRFLOW_VERSION
            ),
            'unknownSourceAttribute': UnknownOperatorAttributeRunFacet(
                unknownItems=[
                    UnknownOperatorInstance(
                        name='DummyOperator',
                        properties=mock.ANY
                    )
                ]
            )
        }),
        job=Job("default", job_id, {
            "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
            "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
        }),
        producer=PRODUCER,
        inputs=[],
        outputs=[]
    ))


def make_parent_run_id(dag_id=DAG_ID, dag_run_id=DAG_RUN_ID):
    return str(uuid.uuid3(uuid.NAMESPACE_URL, f'{dag_id}.{dag_run_id}'))


class TestFixtureHookingDummyOperator(DummyOperator):

    @apply_defaults
    def __init__(self, *args, result=None, **kwargs):
        super(TestFixtureHookingDummyOperator, self).__init__(*args, **kwargs)
        self.result = result

    def execute(self, context):
        return self.result
