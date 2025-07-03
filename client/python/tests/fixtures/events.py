# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
from typing import Dict, List

from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.uuid import generate_new_uuid


def create_run_event(
    event_type: RunState,
    run_id: str | None = None,
    job_name: str = "test-job",
    job_namespace: str = "test-namespace",
    event_time: str | None = None,
    inputs: List[Dataset] | None = None,
    outputs: List[Dataset] | None = None,
) -> RunEvent:
    """Create a RunEvent for testing purposes."""
    if run_id is None:
        run_id = str(generate_new_uuid())

    if event_time is None:
        event_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if inputs is None:
        inputs = []

    if outputs is None:
        outputs = []

    return RunEvent(
        eventType=event_type,
        eventTime=event_time,
        run=Run(runId=run_id),
        job=Job(namespace=job_namespace, name=job_name),
        inputs=inputs,
        outputs=outputs,
        producer="test-producer",
        schemaURL="https://openlineage.io/spec/0.0.1/OpenLineage.json",
    )


def create_test_dataset(name: str = "test-dataset", namespace: str = "test-namespace") -> Dataset:
    """Create a Dataset for testing purposes."""
    return Dataset(namespace=namespace, name=name)


def create_run_lifecycle_events(
    run_id: str | None = None,
    job_name: str = "test-job",
    job_namespace: str = "test-namespace",
    include_inputs_outputs: bool = False,
) -> Dict[str, RunEvent]:
    """Create a complete set of run lifecycle events (START, COMPLETE)."""
    if run_id is None:
        run_id = str(generate_new_uuid())

    base_time = datetime.datetime.now(datetime.timezone.utc)

    inputs = []
    outputs = []
    if include_inputs_outputs:
        inputs = [create_test_dataset("input-dataset")]
        outputs = [create_test_dataset("output-dataset")]

    return {
        "start": create_run_event(
            event_type=RunState.START,
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            event_time=base_time.isoformat(),
            inputs=inputs,
            outputs=outputs,
        ),
        "complete": create_run_event(
            event_type=RunState.COMPLETE,
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            event_time=(base_time + datetime.timedelta(seconds=5)).isoformat(),
            inputs=inputs,
            outputs=outputs,
        ),
    }


def create_failed_run_events(
    run_id: str | None = None, job_name: str = "test-job", job_namespace: str = "test-namespace"
) -> Dict[str, RunEvent]:
    """Create a set of run events for a failed run (START, FAIL)."""
    if run_id is None:
        run_id = str(generate_new_uuid())

    base_time = datetime.datetime.now(datetime.timezone.utc)

    return {
        "start": create_run_event(
            event_type=RunState.START,
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            event_time=base_time.isoformat(),
        ),
        "fail": create_run_event(
            event_type=RunState.FAIL,
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            event_time=(base_time + datetime.timedelta(seconds=3)).isoformat(),
        ),
    }


def create_multiple_runs(
    num_runs: int = 3, job_name_prefix: str = "test-job", job_namespace: str = "test-namespace"
) -> List[Dict[str, RunEvent]]:
    """Create multiple complete run lifecycles for testing concurrency."""
    runs = []
    for i in range(num_runs):
        run_events = create_run_lifecycle_events(
            job_name=f"{job_name_prefix}-{i}", job_namespace=job_namespace
        )
        runs.append(run_events)
    return runs


# Event templates for different scenarios
BASIC_RUN_EVENTS = create_run_lifecycle_events()
FAILED_RUN_EVENTS = create_failed_run_events()
MULTIPLE_RUNS = create_multiple_runs(num_runs=5)
