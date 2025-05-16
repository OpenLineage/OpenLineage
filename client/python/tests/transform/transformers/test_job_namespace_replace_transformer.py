# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime

import pytest
from openlineage.client.event_v2 import DatasetEvent, Job, JobEvent, Run, RunEvent, RunState
from openlineage.client.generated.base import StaticDataset
from openlineage.client.transport.transform.transformers.job_namespace_replace_transformer import (
    JobNamespaceReplaceTransformer,
)
from openlineage.client.uuid import generate_new_uuid


def test_transformer_requires_new_namespace():
    JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_value"})

    with pytest.raises(RuntimeError):
        JobNamespaceReplaceTransformer(properties={})


def test_transformer_early_exit_for_unsupported_event_type():
    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_value"})

    unsupported_event = DatasetEvent(
        eventTime=datetime.datetime.now().isoformat(),
        producer="prod",
        dataset=StaticDataset(namespace="namespace", name="name"),
    )

    result = transformer.transform(unsupported_event)

    assert result is unsupported_event


def test_transformer_replaces_namespace_for_run_event():
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_namespace"})
    result = transformer.transform(event)

    assert result.job.namespace == "new_namespace"


def test_transformer_replaces_namespace_for_job_event():
    event = JobEvent(
        eventTime=datetime.datetime.now().isoformat(),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_namespace"})
    result = transformer.transform(event)

    assert result.job.namespace == "new_namespace"


def test_transformer_does_not_modify_other_fields():
    run_id = str(generate_new_uuid())
    event = RunEvent(
        eventType=RunState.START,
        eventTime="2024-01-01T00:00:00Z",
        run=Run(runId=run_id),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_namespace"})
    transformed_event = transformer.transform(event)

    assert transformed_event.job.namespace == "new_namespace"
    assert transformed_event.job.name == "test"
    assert transformed_event.run.runId == run_id
    assert transformed_event.eventType == RunState.START
    assert transformed_event.eventTime == "2024-01-01T00:00:00Z"
    assert transformed_event.producer == "prod"
