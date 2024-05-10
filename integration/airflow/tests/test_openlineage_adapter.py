# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import datetime as dt
import logging
import os
import uuid
from unittest import mock
from unittest.mock import ANY, MagicMock, patch

from openlineage.airflow.adapter import _PRODUCER, OpenLineageAdapter
from openlineage.airflow.extractors import TaskMetadata
from openlineage.client.facet import (
    DocumentationJobFacet,
    ExternalQueryRunFacet,
    JobTypeJobFacet,
    NominalTimeRunFacet,
    OwnershipJobFacet,
    OwnershipJobFacetOwners,
    ParentRunFacet,
    ProcessingEngineRunFacet,
    SqlJobFacet,
)
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid


@patch.dict(os.environ, {"MARQUEZ_URL": "http://marquez:5000", "MARQUEZ_API_KEY": "api-key"})
def test_create_client_from_marquez_url():
    client = OpenLineageAdapter().get_or_create_openlineage_client()
    assert client.transport.url == "http://marquez:5000"


@patch.dict(
    os.environ,
    {"OPENLINEAGE_URL": "http://ol-api:5000", "OPENLINEAGE_API_KEY": "api-key"},
)
def test_create_client_from_ol_env():
    client = OpenLineageAdapter().get_or_create_openlineage_client()
    assert client.transport.url == "http://ol-api:5000"


def test_setting_ol_adapter_log_level() -> None:
    # DEBUG level set for `openlineage` logger in tests setup
    default_log_level = logging.DEBUG
    # without environment variable
    OpenLineageAdapter()
    parent_logger = logging.getLogger("openlineage.airflow")
    logger = logging.getLogger("openlineage.airflow.adapter")
    assert parent_logger.getEffectiveLevel() == default_log_level
    assert logger.getEffectiveLevel() == default_log_level
    with patch.dict(os.environ, {"OPENLINEAGE_AIRFLOW_LOGGING": "CRITICAL"}):
        assert parent_logger.getEffectiveLevel() == default_log_level
        assert logger.getEffectiveLevel() == default_log_level
        OpenLineageAdapter()
        assert parent_logger.getEffectiveLevel() == logging.CRITICAL
        assert logger.getEffectiveLevel() == logging.CRITICAL


@patch("openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client")
@patch("openlineage.airflow.adapter.redact_with_exclusions")
@patch("openlineage.airflow.adapter.Stats.incr")
@patch("openlineage.airflow.adapter.Stats.timer")
def test_openlineage_adapter_stats_emit_success(
    mock_stats_timer, mock_stats_incr, mock_redact, mock_get_client
):
    adapter = OpenLineageAdapter()

    adapter.emit(MagicMock())

    mock_stats_incr.assert_not_called()
    mock_stats_timer.assert_called_with("ol.emit.attempts")


@patch("openlineage.airflow.adapter.OpenLineageAdapter.get_or_create_openlineage_client")
@patch("openlineage.airflow.adapter.redact_with_exclusions")
@patch("openlineage.airflow.adapter.Stats.incr")
@patch("openlineage.airflow.adapter.Stats.timer")
def test_openlineage_adapter_stats_emit_failed(
    mock_stats_timer, mock_stats_incr, mock_redact, mock_get_client
):
    adapter = OpenLineageAdapter()
    mock_get_client.return_value.emit.side_effect = Exception()

    adapter.emit(MagicMock())

    mock_stats_timer.assert_called_with("ol.emit.attempts")
    mock_stats_incr.assert_has_calls([mock.call("ol.emit.failed")])


def test_emit_start_event():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = datetime.datetime.now().isoformat()
    adapter.start_task(
        run_id=run_id,
        job_name="job",
        job_description="description",
        event_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        code_location=None,
        nominal_start_time=datetime.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=datetime.datetime(2022, 1, 1).isoformat(),
        owners=[],
        task=None,
        run_facets=None,
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime="2022-01-01T00:00:00",
                        nominalEndTime="2022-01-01T00:00:00",
                    ),
                    "processing_engine": ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                },
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "documentation": DocumentationJobFacet(description="description"),
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK"),
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
    )


def test_emit_start_event_with_additional_information():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = dt.datetime.now().isoformat()
    adapter.start_task(
        run_id=run_id,
        job_name="job",
        job_description="description",
        event_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        code_location=None,
        nominal_start_time=dt.datetime(2022, 1, 1).isoformat(),
        nominal_end_time=dt.datetime(2022, 1, 1).isoformat(),
        owners=["owner1", "owner2"],
        task=TaskMetadata(
            name="task_metadata",
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
            run_facets={"externalQuery1": ExternalQueryRunFacet(externalQueryId="123", source="source")},
        ),
        run_facets={"externalQuery2": ExternalQueryRunFacet(externalQueryId="999", source="source")},
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={
                    "nominalTime": NominalTimeRunFacet(
                        nominalStartTime="2022-01-01T00:00:00",
                        nominalEndTime="2022-01-01T00:00:00",
                    ),
                    "processing_engine": ProcessingEngineRunFacet(
                        version=ANY, name="Airflow", openlineageAdapterVersion=ANY
                    ),
                    "parent": ParentRunFacet(
                        run={"runId": "parent_run_id"},
                        job={"namespace": "default", "name": "parent_job_name"},
                    ),
                    "externalQuery1": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                    "externalQuery2": ExternalQueryRunFacet(externalQueryId="999", source="source"),
                },
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "documentation": DocumentationJobFacet(description="description"),
                    "ownership": OwnershipJobFacet(
                        owners=[
                            OwnershipJobFacetOwners(name="owner1", type=None),
                            OwnershipJobFacetOwners(name="owner2", type=None),
                        ]
                    ),
                    "sql": SqlJobFacet(query="SELECT 1;"),
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK"),
                },
            ),
            producer=_PRODUCER,
            inputs=[
                Dataset(namespace="bigquery", name="a.b.c"),
                Dataset(namespace="bigquery", name="x.y.z"),
            ],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
        )
    )


def test_emit_complete_event():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = datetime.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        job_name="job",
        task=TaskMetadata(name="task_metadata"),
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={},
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK")
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
    )


def test_emit_complete_event_with_additional_information():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = dt.datetime.now().isoformat()
    adapter.complete_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        job_name="job",
        task=TaskMetadata(
            name="task_metadata",
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
            run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source")},
        ),
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={
                    "parent": ParentRunFacet(
                        run={"runId": "parent_run_id"},
                        job={"namespace": "default", "name": "parent_job_name"},
                    ),
                    "externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                },
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "sql": SqlJobFacet(query="SELECT 1;"),
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK"),
                },
            ),
            producer=_PRODUCER,
            inputs=[
                Dataset(namespace="bigquery", name="a.b.c"),
                Dataset(namespace="bigquery", name="x.y.z"),
            ],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
        )
    )


def test_emit_fail_event():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = datetime.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name=None,
        parent_run_id=None,
        job_name="job",
        task=TaskMetadata(name="task_metadata"),
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={},
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK")
                },
            ),
            producer=_PRODUCER,
            inputs=[],
            outputs=[],
        )
    )


def test_emit_fail_event_with_additional_information():
    adapter = OpenLineageAdapter()
    adapter.emit = mock.Mock()

    run_id = str(generate_new_uuid())
    event_time = dt.datetime.now().isoformat()
    adapter.fail_task(
        run_id=run_id,
        end_time=event_time,
        parent_job_name="parent_job_name",
        parent_run_id="parent_run_id",
        job_name="job",
        task=TaskMetadata(
            name="task_metadata",
            inputs=[Dataset(namespace="bigquery", name="a.b.c"), Dataset(namespace="bigquery", name="x.y.z")],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
            job_facets={"sql": SqlJobFacet(query="SELECT 1;")},
            run_facets={"externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source")},
        ),
    )

    adapter.emit.assert_called_once_with(
        RunEvent(
            eventType=RunState.FAIL,
            eventTime=event_time,
            run=Run(
                runId=run_id,
                facets={
                    "parent": ParentRunFacet(
                        run={"runId": "parent_run_id"},
                        job={"namespace": "default", "name": "parent_job_name"},
                    ),
                    "externalQuery": ExternalQueryRunFacet(externalQueryId="123", source="source"),
                },
            ),
            job=Job(
                namespace="default",
                name="job",
                facets={
                    "sql": SqlJobFacet(query="SELECT 1;"),
                    "jobType": JobTypeJobFacet(processingType="BATCH", integration="AIRFLOW", jobType="TASK"),
                },
            ),
            producer=_PRODUCER,
            inputs=[
                Dataset(namespace="bigquery", name="a.b.c"),
                Dataset(namespace="bigquery", name="x.y.z"),
            ],
            outputs=[Dataset(namespace="gs://bucket", name="exported_folder")],
        )
    )


def test_build_dag_run_id_is_valid_uuid():
    dag_id = "test_dag"
    execution_date = datetime.datetime.now()
    result = OpenLineageAdapter.build_dag_run_id(
        dag_id=dag_id,
        execution_date=execution_date,
    )
    uuid_result = uuid.UUID(result)
    assert uuid_result
    assert uuid_result.version == 7


def test_build_dag_run_id_different_inputs_give_different_results():
    result1 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag1",
        execution_date=datetime.datetime.now(),
    )
    result2 = OpenLineageAdapter.build_dag_run_id(
        dag_id="dag2",
        execution_date=datetime.datetime.now(),
    )
    assert result1 != result2


def test_build_task_instance_run_id_is_valid_uuid():
    result = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        execution_date=datetime.datetime.now(),
    )
    uuid_result = uuid.UUID(result)
    assert uuid_result
    assert uuid_result.version == 7


def test_build_task_instance_run_id_different_inputs_gives_different_results():
    result1 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag1",
        task_id="task1",
        try_number=1,
        execution_date=datetime.datetime.now(),
    )
    result2 = OpenLineageAdapter.build_task_instance_run_id(
        dag_id="dag2",
        task_id="task2",
        try_number=2,
        execution_date=datetime.datetime.now(),
    )
    assert result1 != result2
