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
import datetime
import functools
import logging
from uuid import UUID

import mock
import pytest
from airflow.models import (TaskInstance, DagRun)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow.version import version as AIRFLOW_VERSION
from marquez.dataset import Source, Dataset
from marquez.models import (
    DbTableName,
    DbTableSchema,
    DbColumn
)
from marquez_airflow import DAG
from marquez_airflow import __version__ as MARQUEZ_AIRFLOW_VERSION
from marquez_airflow.extractors import (
    BaseExtractor, StepMetadata
)
from marquez_airflow.extractors.extractors import Extractors
from marquez_airflow.facets import AirflowRunArgsRunFacet, \
    AirflowVersionRunFacet
from marquez_airflow.utils import get_location, get_job_name, new_lineage_run_id
from openlineage.facet import NominalTimeRunFacet, SourceCodeLocationJobFacet, \
    DocumentationJobFacet, DataSourceDatasetFacet, SchemaDatasetFacet, \
    SchemaField, ParentRunFacet, SqlJobFacet
from openlineage.run import RunEvent, RunState, Job, Run, \
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
    'A simple DAG to test the marquez.DAG metadata extraction flow.'

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

PRODUCER = f"marquez-airflow/{MARQUEZ_AIRFLOW_VERSION}"


@pytest.fixture
@provide_session
def clear_db_airflow_dags(session=None):
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()


@provide_session
def test_new_lineage_run_id(clear_db_airflow_dags, session=None):
    run_id = new_lineage_run_id("dag_id", "task_id")
    assert UUID(run_id).version == 4


def run_id_mock(*args):
    """
    Generates a deterministic run_id so that asserting executed calls is easier
    :param args:
    :return:
    """
    return f"{args[0]}.{args[1]}"


# tests a simple workflow with default extraction mechanism
@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.dag.get_custom_facets')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@mock.patch('marquez_airflow.dag.JobIdMapping')
@provide_session
def test_marquez_dag(job_id_mapping, mock_get_or_create_openlineage_client,
                     get_custom_facets, new_lineage_run_id, clear_db_airflow_dags, session=None):
    dag = DAG(
        DAG_ID,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )
    # (1) Mock the marquez client method calls
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client
    run_id_completed = f"{DAG_RUN_ID}.{TASK_ID_COMPLETED}"
    run_id_failed = f"{DAG_RUN_ID}.{TASK_ID_FAILED}"
    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = run_id_mock

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

    emit_calls = [
        mock.call(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id_completed, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=DAG_RUN_ID,
                    namespace=DAG_NAMESPACE,
                    name=f"{DAG_ID}.{TASK_ID_COMPLETED}"
                )
            }),
            job=Job("default", f"{DAG_ID}.{TASK_ID_COMPLETED}", {
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
                    runId=DAG_RUN_ID,
                    namespace=DAG_NAMESPACE,
                    name=f"{DAG_ID}.{TASK_ID_FAILED}"
                )
            }),
            job=Job("default", f"{DAG_ID}.{TASK_ID_FAILED}", {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", failed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ]
    log.info(
        f"{ [name for name, args, kwargs in mock_marquez_client.mock_calls]}")
    mock_marquez_client.emit.assert_has_calls(emit_calls)

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
            run=Run(run_id_completed),
            job=Job("default", f"{DAG_ID}.{TASK_ID_COMPLETED}"),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        )),
        mock.call(RunEvent(
            eventType=RunState.FAIL,
            eventTime=mock.ANY,
            run=Run(run_id_failed),
            job=Job("default", f"{DAG_ID}.{TASK_ID_FAILED}"),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ]
    mock_marquez_client.emit.assert_has_calls(emit_calls)


@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@provide_session
def test_lineage_run_id(mock_get_or_create_openlineage_client, session=None):
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client

    dag = DAG(
        "test_lineage_run_id",
        schedule_interval="@daily",
        default_args=DAG_DEFAULT_ARGS,
        description="test dag"
    )

    class Collector:

        def update_task_id(self, tid):
            self.task_id = tid
            print(f"Got task id {self.task_id}")

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
    assert collector.task_id != ""


class TestFixtureDummyOperator(DummyOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(TestFixtureDummyOperator, self).__init__(*args, **kwargs)


class TestFixtureDummyExtractor(BaseExtractor):
    operator_class = TestFixtureDummyOperator
    source = Source(
        scheme="dummy",
        authority="localhost:1234",
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> StepMetadata:
        inputs = [
            Dataset.from_table(self.source, "extract_input1")
        ]
        outputs = [
            Dataset.from_table(self.source, "extract_output1")
        ]
        return StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs,
            context={
                "extract": "extract"
            }
        )

    def extract_on_complete(self, task_instance) -> StepMetadata:
        return None


class TestFixtureDummyExtractorOnComplete(BaseExtractor):
    operator_class = TestFixtureDummyOperator
    source = Source(
        scheme="dummy",
        authority="localhost:1234",
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> StepMetadata:
        return None

    def extract_on_complete(self, task_instance) -> StepMetadata:
        inputs = [
            Dataset.from_table_schema(self.source, DbTableSchema(
                schema_name='schema',
                table_name=DbTableName('extract_on_complete_input1'),
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
            ))
        ]
        outputs = [
            Dataset.from_table(self.source, "extract_on_complete_output1")
        ]
        return StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=outputs,
            context={
                "extract_on_complete": "extract_on_complete"
            }
        )


# test the lifecycle including with extractors
@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.dag.get_custom_facets')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@mock.patch('marquez_airflow.dag.JobIdMapping')
@provide_session
def test_marquez_dag_with_extractor(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    extractor_mapper = Extractors()
    extractor_mapper.extractors[TestFixtureDummyOperator] = TestFixtureDummyExtractor

    dag_id = 'test_marquez_dag_with_extractor'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION,
        extractor_mapper=extractor_mapper
    )

    dag_run_id = 'test_marquez_dag_with_extractor_run_id'

    run_id = f"{dag_run_id}.{TASK_ID_COMPLETED}"
    # Mock the marquez client method calls
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = run_id_mock

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

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.START,
            mock.ANY,
            Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=dag_run_id,
                    namespace=DAG_NAMESPACE,
                    name=f"{dag_id}.{TASK_ID_COMPLETED}"
                )
            }),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}", {
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

    mock_marquez_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    # --- Assert that the openlineage call is done

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.COMPLETE,
            mock.ANY,
            Run(run_id),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}"),
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


@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.dag.get_custom_facets')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@mock.patch('marquez_airflow.dag.JobIdMapping')
@provide_session
def test_marquez_dag_with_extract_on_complete(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    extractor_mapper = Extractors()
    extractor_mapper.extractors[TestFixtureDummyOperator] = TestFixtureDummyExtractorOnComplete

    dag_id = 'test_marquez_dag_with_extractor_on_complete'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION,
        extractor_mapper=extractor_mapper
    )

    dag_run_id = 'test_marquez_dag_with_extractor_run_id'
    run_id = f"{dag_run_id}.{TASK_ID_COMPLETED}"
    # Mock the marquez client method calls
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = run_id_mock

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

    mock_marquez_client.emit.assert_has_calls([
        mock.call(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=dag_run_id,
                    namespace=DAG_NAMESPACE,
                    name=f"{dag_id}.{TASK_ID_COMPLETED}"
                )
            }),
            job=Job("default",  f"{dag_id}.{TASK_ID_COMPLETED}", {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
        ))
    ])

    mock_marquez_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    mock_marquez_client.emit.assert_has_calls([
        mock.call(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=mock.ANY,
            run=Run(run_id),
            job=Job("default", f"{dag_id}.{TASK_ID_COMPLETED}"),
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


class TestFixtureDummyExtractorWithMultipleSteps(BaseExtractor):
    operator_class = TestFixtureDummyOperator
    source = Source(
        scheme='dummy',
        authority='localhost:1234',
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)

    def extract(self) -> [StepMetadata]:
        inputs = [
            Dataset.from_table(self.source, "extract_input1")
        ]
        outputs = [
            Dataset.from_table(self.source, "extract_output1")
        ]
        return [StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=inputs,
            outputs=None,
            context={
                "phase": "input",
                "extract": "extract"
            }
        ), StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=None,
            outputs=outputs,
            context={
                "phase": "output",
                "extract": "extract"
            }
        )]

    def extract_on_complete(self, task_instance) -> StepMetadata:
        return None


# test the lifecycle including with extractors
@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.dag.get_custom_facets')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@mock.patch('marquez_airflow.dag.JobIdMapping')
@provide_session
def test_marquez_dag_with_extractor_returning_two_steps(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    extractor_mapper = Extractors()
    extractor_mapper.extractors[TestFixtureDummyOperator] = \
        TestFixtureDummyExtractorWithMultipleSteps

    dag_id = 'test_marquez_dag_with_extractor_returning_two_steps'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION,
        extractor_mapper=extractor_mapper
    )

    dag_run_id = 'test_marquez_dag_with_extractor_returning_two_steps_run_id'
    run_id = f"{dag_run_id}.{TASK_ID_COMPLETED}"

    # Mock the marquez client method calls
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = run_id_mock

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

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.START,
            mock.ANY,
            Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=dag_run_id,
                    namespace=DAG_NAMESPACE,
                    name=f"{dag_id}.{TASK_ID_COMPLETED}"
                )
            }),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}", {
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
            []
        )
    )

    mock_marquez_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    # --- Assert that the openlineage call is done

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.COMPLETE,
            mock.ANY,
            Run(run_id),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}"),
            PRODUCER,
            [OpenLineageDataset('dummy://localhost:1234', 'extract_input1', {
                "dataSource": DataSourceDatasetFacet(
                    name='dummy://localhost:1234',
                    uri='dummy://localhost:1234?query_tag=asdf'
                )
            })],
            []
        )
    )


# tests a simple workflow with default custom facet mechanism
@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
def test_marquez_dag_adds_custom_facets(
        mock_get_or_create_openlineage_client,
        new_lineage_run_id,
        clear_db_airflow_dags,
):

    dag = DAG(
        DAG_ID,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION
    )
    # Mock the marquez client method calls
    mock_openlineage_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_openlineage_client
    run_id_completed = f"{DAG_RUN_ID}.{TASK_ID_COMPLETED}"
    new_lineage_run_id.side_effect = run_id_mock

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

    mock_openlineage_client.emit.assert_called_once_with(RunEvent(
            eventType=RunState.START,
            eventTime=mock.ANY,
            run=Run(run_id_completed, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=DAG_RUN_ID,
                    namespace=DAG_NAMESPACE,
                    name=f"{DAG_ID}.{TASK_ID_COMPLETED}"
                ),
                "airflow_runArgs": AirflowRunArgsRunFacet(False),
                "airflow_version": AirflowVersionRunFacet(
                    operator="airflow.operators.dummy_operator.DummyOperator",
                    taskInfo=mock.ANY,
                    airflowVersion=AIRFLOW_VERSION,
                    marquezAirflowVersion=MARQUEZ_AIRFLOW_VERSION
                )
            }),
            job=Job("default", f"{DAG_ID}.{TASK_ID_COMPLETED}", {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            producer=PRODUCER,
            inputs=[],
            outputs=[]
    ))


class TestFixtureHookingDummyOperator(DummyOperator):

    @apply_defaults
    def __init__(self, *args, result=None, **kwargs):
        super(TestFixtureHookingDummyOperator, self).__init__(*args, **kwargs)
        self.result = result

    def execute(self, context):
        return self.result


def wrap_callback(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        result = f(self, *args, **kwargs)
        self._extractor.store_result(result)
        return result
    return wrapper


TestFixtureHookingDummyOperator.execute = wrap_callback(TestFixtureHookingDummyOperator.execute)


class TestFixtureHookingDummyExtractor(BaseExtractor):
    operator_class = TestFixtureHookingDummyOperator
    source = Source(
        scheme="dummy://localhost:1234",
        connection_url="dummy://localhost:1234?query_tag=asdf"
    )

    def __init__(self, operator):
        super().__init__(operator)
        self.operator._extractor = self
        self.result = None

    def store_result(self, result):
        self.result = result

    def extract(self) -> StepMetadata:
        return None

    def extract_on_complete(self, task_instance) -> StepMetadata:
        return StepMetadata(
            name=get_job_name(task=self.operator),
            inputs=[],
            outputs=[],
            context={
                "sql": self.result
            }
        )


# tests a simple workflow with default custom facet mechanism
# test the lifecycle including with extractors
@mock.patch('marquez_airflow.dag.new_lineage_run_id')
@mock.patch('marquez_airflow.dag.get_custom_facets')
@mock.patch('marquez_airflow.marquez.MarquezAdapter.get_or_create_openlineage_client')
@mock.patch('marquez_airflow.dag.JobIdMapping')
@provide_session
def test_marquez_dag_with_hooking_operator(
        job_id_mapping,
        mock_get_or_create_openlineage_client,
        get_custom_facets,
        new_lineage_run_id,
        clear_db_airflow_dags,
        session=None):

    # --- test setup

    # Add the dummy extractor to the list for the task above
    extractor_mapper = Extractors()
    extractor_mapper.extractors[TestFixtureHookingDummyOperator] = TestFixtureHookingDummyExtractor

    dag_id = 'test_marquez_dag_with_extractor_returning_two_steps'
    dag = DAG(
        dag_id,
        schedule_interval='@daily',
        default_args=DAG_DEFAULT_ARGS,
        description=DAG_DESCRIPTION,
        extractor_mapper=extractor_mapper
    )

    dag_run_id = 'test_marquez_dag_with_extractor_returning_two_steps_run_id'
    run_id = f"{dag_run_id}.{TASK_ID_COMPLETED}"

    # Mock the marquez client method calls
    mock_marquez_client = mock.Mock()
    mock_get_or_create_openlineage_client.return_value = mock_marquez_client
    get_custom_facets.return_value = {}
    new_lineage_run_id.side_effect = run_id_mock

    query = "select * from employees"

    # Add task that will be marked as completed
    task_will_complete = TestFixtureHookingDummyOperator(
        task_id=TASK_ID_COMPLETED,
        result=query,
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

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.START,
            mock.ANY,
            Run(run_id, {
                "nominalTime": NominalTimeRunFacet(start_time, end_time),
                "parentRun": ParentRunFacet.create(
                    runId=dag_run_id,
                    namespace=DAG_NAMESPACE,
                    name=f"{dag_id}.{TASK_ID_COMPLETED}"
                )
            }),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}", {
                "documentation": DocumentationJobFacet(DAG_DESCRIPTION),
                "sourceCodeLocation": SourceCodeLocationJobFacet("", completed_task_location)
            }),
            PRODUCER,
            [],
            []
        )
    )

    mock_marquez_client.reset_mock()

    # --- Pretend complete the task
    job_id_mapping.pop.return_value = run_id

    task_will_complete.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    dag.handle_callback(dagrun, success=True, session=session)

    # --- Assert that the openlineage call is done

    mock_marquez_client.emit.assert_called_once_with(
        RunEvent(
            RunState.COMPLETE,
            mock.ANY,
            Run(run_id),
            Job("default", f"{dag_id}.{TASK_ID_COMPLETED}", {
                "sql": SqlJobFacet(query)
            }),
            PRODUCER,
            [],
            []
        )
    )
