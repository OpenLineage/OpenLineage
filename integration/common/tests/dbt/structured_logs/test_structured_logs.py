# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
import attr
from enum import Enum
import json
from typing import Dict

from openlineage.common.provider.dbt.structured_logs import DbtStructuredLogsProcessor
from openlineage.common.test import match

from openlineage.common.provider.dbt.processor import Adapter

###########
# helpers
###########

def ol_event_to_dict(event) -> Dict:
    return attr.asdict(event, value_serializer=serialize)

def serialize(inst, field, value):
    if isinstance(value, Enum):
        return value.value
    return value


##################
# fixtures
##################

@pytest.fixture(autouse=True)
def patch_get_dbt_profiles_dir(monkeypatch):
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.get_dbt_profiles_dir",
        lambda *args, **kwargs: "./tests/dbt/structured_logs"
    )

##################
# test functions
##################

@pytest.mark.parametrize(
    "target, command_line, logs_path, expected_ol_events_path, manifest_path",
    [
        # successful postgres run
        (
            "postgres",
            ["dbt", "run", "..."],
            "./tests/dbt/structured_logs/postgres/run/logs/successful_run_logs.jsonl",
            "./tests/dbt/structured_logs/postgres/run/results/successful_run_ol_events.json",
            "./tests/dbt/structured_logs/postgres/run/target/manifest.json"
        ),

        # failed postgres run. Model has SQL error in it
        (
            "postgres",
            ["dbt", "run", "..."],
            "./tests/dbt/structured_logs/postgres/run/logs/failed_run_logs.jsonl",
            "./tests/dbt/structured_logs/postgres/run/results/failed_run_ol_events.json",
            "./tests/dbt/structured_logs/postgres/run/target/manifest.json"
        ),

        # successful snowflake run
        (
            "snowflake",
            ["dbt", "run", "..."],
            "./tests/dbt/structured_logs/snowflake/run/logs/successful_run_logs.jsonl",
            "./tests/dbt/structured_logs/snowflake/run/results/successful_run_ol_events.json",
            "./tests/dbt/structured_logs/snowflake/run/target/manifest.json"
        ),
        # failed snowflake run
        (
            "snowflake",
            ["dbt", "run", "..."],
            "./tests/dbt/structured_logs/snowflake/run/logs/failed_run_logs.jsonl",
            "./tests/dbt/structured_logs/snowflake/run/results/failed_run_ol_events.json",
            "./tests/dbt/structured_logs/snowflake/run/target/manifest.json"
        ),
        # postgres seed
        (
                "postgres",
                ["dbt", "seed", "..."],
                "./tests/dbt/structured_logs/postgres/seed/logs/seed_logs.jsonl",
                "./tests/dbt/structured_logs/postgres/seed/results/seed_ol_events.json",
                "./tests/dbt/structured_logs/postgres/seed/target/manifest.json"
        ),
        # snowflake seed
        (
                "snowflake",
                ["dbt", "seed", "..."],
                "./tests/dbt/structured_logs/snowflake/seed/logs/seed_logs.jsonl",
                "./tests/dbt/structured_logs/snowflake/seed/results/seed_ol_events.json",
                "./tests/dbt/structured_logs/snowflake/seed/target/manifest.json"
        ),
        # postgres snapshot
        (
                "postgres",
                ["dbt", "snapshot", "..."],
                "./tests/dbt/structured_logs/postgres/snapshot/logs/snapshot_logs.jsonl",
                "./tests/dbt/structured_logs/postgres/snapshot/results/snapshot_ol_events.json",
                "./tests/dbt/structured_logs/postgres/snapshot/target/manifest.json"
        ),
        # snowflake snapshot
        (
                "snowflake",
                ["dbt", "snapshot", "..."],
                "./tests/dbt/structured_logs/snowflake/snapshot/logs/snapshot_logs.jsonl",
                "./tests/dbt/structured_logs/snowflake/snapshot/results/snapshot_ol_events.json",
                "./tests/dbt/structured_logs/snowflake/snapshot/target/manifest.json"
        )
    ],
    ids=[
        # run command
        "postgres_successful_dbt_run", "postgres_failed_dbt_run",
        "snowflake_successful_dbt_run", "snowflake_failed_dbt_run",
        # seed command
        "postgres_dbt_seed",  "snowflake_dbt_seed",
        # snapshot command
        "postgres_dbt_snapshot", "snowflake_dbt_snapshot"

    ]
)
def test_parse(target, command_line, logs_path, expected_ol_events_path, manifest_path, monkeypatch):
    def dummy_run_dbt_command(self):
        return open(logs_path).readlines()

    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command",
        dummy_run_dbt_command
    )


    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir="tests/dbt/structured_logs",
        target=target,
        dbt_command_line=command_line,
    )

    processor.manifest_path = manifest_path


    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = json.load(open(expected_ol_events_path))

    assert match(expected=expected_ol_events, result=actual_ol_events)


@pytest.mark.parametrize(
    "target, expected_adapter_type",
    [
        ("postgres", Adapter.POSTGRES),
        ("snowflake", Adapter.SNOWFLAKE),

    ],
    ids=["postgres", "snowflake"]
)
def test_adapter_type(target, expected_adapter_type, monkeypatch):
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command",
        lambda *args, **kwargs: []
    )

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir="tests/dbt/structured_logs",
        target=target,
        dbt_command_line=["dbt", "run", "..."]
    )

    try:
        next(processor.parse())
    except StopIteration:
        pass

    assert processor.adapter_type == expected_adapter_type


@pytest.mark.parametrize(
    "target, expected_dataset_namespace",
    [
        ("postgres", "postgres://postgres:5432"),
        ("snowflake", "snowflake://mysnowflakeaccount.us-east-1.aws"),

    ],
    ids=["postgres", "snowflake"]
)
def test_dataset_namespace(target, expected_dataset_namespace, monkeypatch):
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command",
        lambda *args, **kwargs: []
    )

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir="tests/dbt/structured_logs",
        target=target,
        dbt_command_line=["dbt", "run", "..."]
    )

    try:
        next(processor.parse())
    except StopIteration:
        pass

    assert processor.dataset_namespace == expected_dataset_namespace
