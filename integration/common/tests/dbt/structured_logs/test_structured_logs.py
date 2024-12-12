import pytest
import attr
from enum import Enum
import json
import yaml
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

def get_profiles_dir() -> str:
    return "./tests/dbt/structured_logs"


##################
# test functions
##################

@pytest.mark.parametrize(
    "target, logs_path, expected_ol_events_path, manifest_path",
    [
        # successful pg run
        (
            "postgres",
            "./tests/dbt/structured_logs/postgres/run/logs/successful_run_logs.jsonl",
            "./tests/dbt/structured_logs/postgres/run/results/successful_run_ol_events.json",
            "./tests/dbt/structured_logs/postgres/run/target/manifest.json"
        ),

        # failed pg run. Model has SQL error in it
        (
            "postgres",
            "./tests/dbt/structured_logs/postgres/run/logs/failed_run_logs.jsonl",
            "./tests/dbt/structured_logs/postgres/run/results/failed_run_ol_events.json",
            "./tests/dbt/structured_logs/postgres/run/target/manifest.json"
        ),

        # successful Snowflake run
        (
            "snowflake",
            "./tests/dbt/structured_logs/snowflake/run/logs/successful_run_logs.jsonl",
            "./tests/dbt/structured_logs/snowflake/run/results/successful_run_ol_events.json",
            "./tests/dbt/structured_logs/snowflake/run/target/manifest.json"
        ),
        # failed snowflake run
        (
            "snowflake",
            "./tests/dbt/structured_logs/snowflake/run/logs/failed_run_logs.jsonl",
            "./tests/dbt/structured_logs/snowflake/run/results/failed_run_ol_events.json",
            "./tests/dbt/structured_logs/snowflake/run/target/manifest.json"
        ),
    ],
    ids=["postgres_successful_dbt_run", "postgres_failed_dbt_run", "snowflake_successful_dbt_run", "snowflake_failed_dbt_run"]
)
def test_parse(target, logs_path, expected_ol_events_path, manifest_path, monkeypatch):
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
        dbt_command_line=f"dbt --log-format json run --profiles-dir {get_profiles_dir()} --target snowflake".split(" ")
    )

    processor.manifest_path = manifest_path


    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = json.load(open(expected_ol_events_path))

    with open("foo.json", "w") as f:
        f.write(json.dumps(actual_ol_events))

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
        dbt_command_line=f"dbt --log-format json run --profiles-dir {get_profiles_dir()} --target snowflake".split(" ")
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
        dbt_command_line=f"dbt --log-format json run --profiles-dir {get_profiles_dir()} --target snowflake".split(" ")
    )

    try:
        next(processor.parse())
    except StopIteration:
        pass

    assert processor.dataset_namespace == expected_dataset_namespace