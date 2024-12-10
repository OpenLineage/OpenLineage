import pytest
import attr
from enum import Enum
import json
from openlineage.common.provider.dbt.local_structured_logging import DbtStructuredLoggingProcessor

from openlineage.common.test import match


@pytest.mark.parametrize(
    "path, logs_path, expected_ol_events_path",
    [
        # successful pg run
        (
            "tests/dbt/structured_logs/run",
            "./tests/dbt/structured_logs/run/logs/pg_successful_run.log",
            "./tests/dbt/structured_logs/run/logs/pg_successful_run_ol_events.json"
        ),

        # failed pg run. Model has SQL error in it
        (
            "tests/dbt/structured_logs/run",
            "./tests/dbt/structured_logs/run/logs/pg_failed_run.log",
            "./tests/dbt/structured_logs/run/logs/pg_failed_run_ol_events.json"
        ),

        # successful Snowflake run
        (
            "tests/dbt/structured_logs/run",
            "./tests/dbt/structured_logs/run/logs/snowflake_successful_run.log",
            "./tests/dbt/structured_logs/run/logs/snowflake_successful_run_ol_events.json"
        ),
        # failed snowflake run
        (
                "tests/dbt/structured_logs/run",
                "./tests/dbt/structured_logs/run/logs/snowflake_failed_run.log",
                "./tests/dbt/structured_logs/run/logs/snowflake_failed_run_ol_events.json"
        ),
    ],
    ids=["pg_successful_dbt_run", "pg_failed_dbt_run", "snowflake_successful_dbt_run", "snowflake_failed_dbt_run"]
)
def test_parse(path, logs_path, expected_ol_events_path, monkeypatch):
    def dummy_run_dbt_command(self):
        return open(logs_path).readlines()

    monkeypatch.setattr(
        "openlineage.common.provider.dbt.local_structured_logging.DbtStructuredLoggingProcessor._run_dbt_command",
        dummy_run_dbt_command
    )

    processor = DbtStructuredLoggingProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=path,
        dbt_command=f"dbt --log-format json run --profiles-dir {get_profiles_dir()}".split(" ")
    )


    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = json.load(open(expected_ol_events_path))

    with open("foo.json", "w") as f:
        f.write(json.dumps(actual_ol_events))

    assert match(expected=expected_ol_events, result=actual_ol_events)


#todo test when there are errors that are triggered as well
############
# fixtures
###########


@pytest.fixture(autouse=True)
def patch_functions(monkeypatch):

    compile_manifest_path = "./tests/dbt/structured_logs/ol-compile-target/manifest.json"
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.local_structured_logging.DbtStructuredLoggingProcessor.compile_manifest_path",
        compile_manifest_path
    )

# todo test the parent/child relationships for the events


############
# helpers
###########

def ol_event_to_dict(event):
    return attr.asdict(event, value_serializer=serialize)

def serialize(inst, field, value):
    if isinstance(value, Enum):
        return value.value
    return value

def get_profiles_dir() -> str:
    return "./tests/dbt/structured_logs"