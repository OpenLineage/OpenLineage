# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from typing import Any, Literal


def validate_event_schema(event: dict[str, Any]) -> bool:
    """Validate that an event has the required OpenLineage schema fields."""
    required_fields = ["eventType", "eventTime", "run", "job", "inputs", "outputs"]

    for field in required_fields:
        if field not in event:
            return False

    # Validate run structure
    if "runId" not in event["run"]:
        return False

    # Validate job structure
    if "namespace" not in event["job"] or "name" not in event["job"]:
        return False

    return True


def filter_events_by_job(events: list[dict[str, Any]], job_name: str) -> list[dict[str, Any]]:
    """Filter events by job name."""
    return [event for event in events if event.get("job", {}).get("name") == job_name]


def get_events_by_type(events: list[dict[str, Any]], event_type: str) -> list[dict[str, Any]]:
    """Get events by event type (START, COMPLETE, FAIL)."""
    return [event for event in events if event.get("eventType") == event_type]


def validate_lineage_chain(events: list[dict[str, Any]], expected_models: list[str]) -> bool:
    """Validate that all expected models appear in the lineage chain."""
    job_names = set()
    for event in events:
        job_name = event.get("job", {}).get("name")
        if job_name:
            job_names.add(job_name)

    for model in expected_models:
        if model not in job_names:
            return False

    return True


def extract_dataset_names(event: dict[str, Any], io_type: str) -> list[str]:
    """Extract dataset names from inputs or outputs."""
    datasets = event.get(io_type, [])
    return [dataset.get("name", "") for dataset in datasets]


def validate_dataset_facets(
    event: dict[str, Any], io_type: Literal["inputs", "outputs"], expected_facets: list[str]
) -> bool:
    """Validate that expected facets are present in dataset inputs/outputs."""
    datasets = event.get(io_type, [])

    for dataset in datasets:
        facets = dataset.get("facets", {})
        for expected_facet in expected_facets:
            if expected_facet not in facets:
                return False

    return True
