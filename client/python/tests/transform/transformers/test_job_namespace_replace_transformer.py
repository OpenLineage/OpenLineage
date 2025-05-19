# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime

import pytest
from openlineage.client.event_v2 import DatasetEvent, Job, JobEvent, Run, RunEvent, RunState
from openlineage.client.facet_v2 import parent_run
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
        run=Run(runId=run_id, facets={"a": "b"}),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_namespace"})
    transformed_event = transformer.transform(event)

    assert transformed_event.job.namespace == "new_namespace"
    assert transformed_event.job.name == "test"
    assert transformed_event.run.runId == run_id
    assert transformed_event.run.facets == {"a": "b"}
    assert transformed_event.eventType == RunState.START
    assert transformed_event.eventTime == "2024-01-01T00:00:00Z"
    assert transformed_event.producer == "prod"


def test_transformer_include_parent_default_value():
    transformer = JobNamespaceReplaceTransformer(properties={"new_job_namespace": "new_namespace"})
    assert transformer.include_parent_facet is True
    assert transformer.new_job_namespace == "new_namespace"


def test_transformer_with_include_parent_true_and_parent_run_facet_absent():
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(runId=str(generate_new_uuid())),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(
        properties={"new_job_namespace": "new_namespace", "include_parent_facet": True}
    )
    result = transformer.transform(event)

    assert result.job.namespace == "new_namespace"


def test_transformer_with_include_parent_true_and_parent_run_facet_present():
    run_id = str(generate_new_uuid())
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "parent": parent_run.ParentRunFacet(
                    run=parent_run.Run(run_id),
                    job=parent_run.Job(namespace="parent_job_namespace", name="parent_job_name"),
                    root=parent_run.Root(
                        run=parent_run.RootRun(run_id),
                        job=parent_run.RootJob(namespace="root_job_namespace", name="root_job_name"),
                    ),
                )
            },
        ),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(
        properties={"new_job_namespace": "new_namespace", "include_parent_facet": True}
    )
    result = transformer.transform(event)

    assert result.job.namespace == "new_namespace"
    assert result.run.facets["parent"] == parent_run.ParentRunFacet(
        run=parent_run.Run(run_id),
        job=parent_run.Job(namespace="new_namespace", name="parent_job_name"),  # Changed
        root=parent_run.Root(
            run=parent_run.RootRun(run_id),
            job=parent_run.RootJob(namespace="new_namespace", name="root_job_name"),  # Changed
        ),
    )


def test_transformer_with_include_parent_false_and_parent_run_facet_present():
    run_id = str(generate_new_uuid())
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.datetime.now().isoformat(),
        run=Run(
            runId=run_id,
            facets={
                "parent": parent_run.ParentRunFacet(
                    run=parent_run.Run(run_id),
                    job=parent_run.Job(namespace="parent_job_namespace", name="parent_job_name"),
                    root=parent_run.Root(
                        run=parent_run.RootRun(run_id),
                        job=parent_run.RootJob(namespace="root_job_namespace", name="root_job_name"),
                    ),
                )
            },
        ),
        job=Job(namespace="old_namespace", name="test"),
        producer="prod",
    )

    transformer = JobNamespaceReplaceTransformer(
        properties={"new_job_namespace": "new_namespace", "include_parent_facet": False}
    )
    result = transformer.transform(event)

    assert result.job.namespace == "new_namespace"
    assert result.run.facets["parent"].job.namespace == "parent_job_namespace"  # Unchanged
    assert result.run.facets["parent"].root.job.namespace == "root_job_namespace"  # Unchanged
    assert result.run.facets["parent"] == parent_run.ParentRunFacet(
        run=parent_run.Run(run_id),
        job=parent_run.Job(namespace="parent_job_namespace", name="parent_job_name"),  # Unchanged
        root=parent_run.Root(
            run=parent_run.RootRun(run_id),
            job=parent_run.RootJob(namespace="root_job_namespace", name="root_job_name"),  # Unchanged
        ),
    )
