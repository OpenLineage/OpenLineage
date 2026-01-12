# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import datetime
import json
from enum import Enum
from pathlib import Path
from typing import Dict
from unittest import mock

import attr
import pytest
import yaml
from openlineage.common.provider.dbt.processor import Adapter
from openlineage.common.provider.dbt.structured_logs import (
    DbtStructuredLogsProcessor,
    ManifestIntegrityResult,
)
from openlineage.common.provider.dbt.utils import DBT_LOG_FILE_MAX_BYTES
from openlineage.common.test import match
from openlineage.common.utils import get_from_nullable_chain

###########
# helpers
###########
DUMMY_UUID_4 = "e2c4a0ab-d119-4828-b9c4-96ffd4c79d4f"
DUMMY_RANDOM_LOG_FILE = "dbt-logs-e2c4a0ab-d119-4828-b9c4-96ffd4c79d4f"
CURRENT_DIR = str(Path(__file__).absolute().parent)


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
        lambda *args, **kwargs: CURRENT_DIR,
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
            CURRENT_DIR + "/postgres/run/logs/successful_run_logs.jsonl",
            CURRENT_DIR + "/postgres/run/results/successful_run_ol_events.json",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        # failed postgres run. Model has SQL error in it
        (
            "postgres",
            ["dbt", "run", "..."],
            CURRENT_DIR + "/postgres/run/logs/failed_run_logs.jsonl",
            CURRENT_DIR + "/postgres/run/results/failed_run_ol_events.json",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        # successful snowflake run
        (
            "snowflake",
            ["dbt", "run", "..."],
            CURRENT_DIR + "/snowflake/run/logs/successful_run_logs.jsonl",
            CURRENT_DIR + "/snowflake/run/results/successful_run_ol_events.json",
            CURRENT_DIR + "/snowflake/run/target/manifest.json",
        ),
        # failed snowflake run
        (
            "snowflake",
            ["dbt", "run", "..."],
            CURRENT_DIR + "/snowflake/run/logs/failed_run_logs.jsonl",
            CURRENT_DIR + "/snowflake/run/results/failed_run_ol_events.json",
            CURRENT_DIR + "/snowflake/run/target/manifest.json",
        ),
        # postgres seed
        (
            "postgres",
            ["dbt", "seed", "..."],
            CURRENT_DIR + "/postgres/seed/logs/seed_logs.jsonl",
            CURRENT_DIR + "/postgres/seed/results/seed_ol_events.json",
            CURRENT_DIR + "/postgres/seed/target/manifest.json",
        ),
        # snowflake seed
        (
            "snowflake",
            ["dbt", "seed", "..."],
            CURRENT_DIR + "/snowflake/seed/logs/seed_logs.jsonl",
            CURRENT_DIR + "/snowflake/seed/results/seed_ol_events.json",
            CURRENT_DIR + "/snowflake/seed/target/manifest.json",
        ),
        # postgres snapshot
        (
            "postgres",
            ["dbt", "snapshot", "..."],
            CURRENT_DIR + "/postgres/snapshot/logs/snapshot_logs.jsonl",
            CURRENT_DIR + "/postgres/snapshot/results/snapshot_ol_events.json",
            CURRENT_DIR + "/postgres/snapshot/target/manifest.json",
        ),
        # snowflake snapshot
        (
            "snowflake",
            ["dbt", "snapshot", "..."],
            CURRENT_DIR + "/snowflake/snapshot/logs/snapshot_logs.jsonl",
            CURRENT_DIR + "/snowflake/snapshot/results/snapshot_ol_events.json",
            CURRENT_DIR + "/snowflake/snapshot/target/manifest.json",
        ),
        # postgres test
        (
            "postgres",
            ["dbt", "test", "..."],
            CURRENT_DIR + "/postgres/test/logs/test_logs.jsonl",
            CURRENT_DIR + "/postgres/test/results/test_ol_events.json",
            CURRENT_DIR + "/postgres/test/target/manifest.json",
        ),
        # postgres dbt tests on sources
        (
            "postgres",
            ["dbt", "test", "..."],
            CURRENT_DIR + "/postgres/test_source/logs/test_source_logs.jsonl",
            CURRENT_DIR + "/postgres/test_source/results/test_source_ol_events.json",
            CURRENT_DIR + "/postgres/test_source/target/manifest.json",
        ),
        # postgres build
        (
            "postgres",
            ["dbt", "build", "..."],
            CURRENT_DIR + "/postgres/build_command/logs/build_logs.jsonl",
            CURRENT_DIR + "/postgres/build_command/results/build_ol_events.json",
            CURRENT_DIR + "/postgres/build_command/target/manifest.json",
        ),
    ],
    ids=[
        # run command
        "postgres_successful_dbt_run",
        "postgres_failed_dbt_run",
        "snowflake_successful_dbt_run",
        "snowflake_failed_dbt_run",
        # seed command
        "postgres_dbt_seed",
        "snowflake_dbt_seed",
        # snapshot command
        "postgres_dbt_snapshot",
        "snowflake_dbt_snapshot",
        # test command
        "postgres_dbt_test",
        "postgres_dbt_test_source",
        # build command
        "postgres_dbt_build",
    ],
)
@mock.patch("openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command")
def test_parse(mock_dbt_run_command, target, command_line, logs_path, expected_ol_events_path, manifest_path):
    def parsed():
        processor.received_dbt_command_completed = True
        return open(logs_path).readlines()

    mock_dbt_run_command.side_effect = parsed

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target=target,
        dbt_command_line=command_line,
    )

    processor.manifest_path = manifest_path

    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = json.load(open(expected_ol_events_path))

    assert match(expected=expected_ol_events, result=actual_ol_events, ordered_list=True)


@pytest.mark.parametrize(
    "target, expected_adapter_type",
    [
        ("postgres", Adapter.POSTGRES),
        ("snowflake", Adapter.SNOWFLAKE),
    ],
    ids=["postgres", "snowflake"],
)
@mock.patch("openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command")
def test_adapter_type(mock_dbt_run_command, target, expected_adapter_type):
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target=target,
        dbt_command_line=["dbt", "run", "..."],
    )

    def parsed():
        processor.received_dbt_command_completed = True
        return []

    mock_dbt_run_command.side_effect = parsed

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
    ids=["postgres", "snowflake"],
)
@mock.patch("openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command")
def test_dataset_namespace(mock_run_dbt_command, target, expected_dataset_namespace):
    def parsed():
        processor.received_dbt_command_completed = True
        return []

    mock_run_dbt_command.side_effect = parsed

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target=target,
        dbt_command_line=["dbt", "run", "..."],
    )

    try:
        next(processor.parse())
    except StopIteration:
        pass

    assert processor.dataset_namespace == expected_dataset_namespace


@pytest.mark.parametrize(
    "dbt_log_events, expected_ol_events, dbt_event_type, manifest_path",
    [
        (
            CURRENT_DIR + "/postgres/events/logs/MainReportVersion.yaml",
            CURRENT_DIR + "/postgres/events/results/MainReportVersion_OL.yaml",
            "MainReportVersion",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/successful_CommandCompleted.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_CommandCompleted_OL.yaml",
            "CommandCompleted",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/failed_CommandCompleted.yaml",
            CURRENT_DIR + "/postgres/events/results/failed_CommandCompleted_OL.yaml",
            "CommandCompleted",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/NodeStart.yaml",
            CURRENT_DIR + "/postgres/events/results/NodeStart_OL.yaml",
            "NodeStart",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/successful_NodeFinished.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_NodeFinished_OL.yaml",
            "NodeFinished",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/failed_NodeFinished.yaml",
            CURRENT_DIR + "/postgres/events/results/failed_NodeFinished_OL.yaml",
            "NodeFinished",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/SQLQuery.yaml",
            CURRENT_DIR + "/postgres/events/results/SQLQuery_OL.yaml",
            "SQLQuery",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/successful_SQLQueryStatus.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_SQLQueryStatus_OL.yaml",
            "SQLQueryStatus",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/failed_SQLQueryStatus.yaml",
            CURRENT_DIR + "/postgres/events/results/failed_SQLQueryStatus_OL.yaml",
            "SQLQueryStatus",
            CURRENT_DIR + "/postgres/run/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/failed_test_NodeFinished.yaml",
            CURRENT_DIR + "/postgres/events/results/failed_test_NodeFinished_OL.yaml",
            "NodeFinished",
            CURRENT_DIR + "/postgres/test/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/successful_test_NodeFinished.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_test_NodeFinished_OL.yaml",
            "NodeFinished",
            CURRENT_DIR + "/postgres/test/target/manifest.json",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/skipped_test_NodeFinished.yaml",
            CURRENT_DIR + "/postgres/events/results/skipped_test_NodeFinished_OL.yaml",
            "NodeFinished",
            CURRENT_DIR + "/postgres/test/target/manifest.json",
        ),
    ],
    ids=[
        "MainReportVersion",
        "successful_CommandCompleted",
        "failed_CommandCompleted",
        "NodeStart",
        "successful_NodeFinished",
        "failed_NodeFinished",
        "SQLQuery",
        "successful_SQLQueryStatus",
        "failed_SQLQueryStatus",
        "failed_test_NodeFinished",
        "successful_test_NodeFinished",
        "skipped_test_NodeFinished",
    ],
)
@mock.patch("openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command")
def test_parse_dbt_events(
    mock_dbt_run_command, dbt_log_events, expected_ol_events, dbt_event_type, manifest_path
):
    """
    This tests:
    1. the parent/child relationship of OL events
    2. the runId remains the same for the START/COMPLETE/FAIL OL events
    """

    def parsed():
        processor.received_dbt_command_completed = True
        return [json.dumps(dbt_event) for dbt_event in yaml.safe_load(open(dbt_log_events))]

    mock_dbt_run_command.side_effect = parsed

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )
    processor.manifest_path = manifest_path
    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = yaml.safe_load(open(expected_ol_events))

    assert match(expected=expected_ol_events, result=actual_ol_events, ordered_list=True)

    command_start_event = actual_ol_events[0]

    if dbt_event_type == "CommandCompleted":
        command_completed_event = actual_ol_events[1]

        assert command_start_event["run"]["runId"] == command_completed_event["run"]["runId"]

    elif dbt_event_type == "NodeStart":
        node_start_event = actual_ol_events[1]

        assert command_start_event["run"]["runId"] == get_from_nullable_chain(
            node_start_event, "run.facets.parent.run.runId".split(".")
        )

    elif dbt_event_type == "NodeFinished":
        node_start_event = actual_ol_events[1]
        node_finished_event = actual_ol_events[2]

        assert node_start_event["run"]["runId"] == node_finished_event["run"]["runId"]

    elif dbt_event_type == "SQLQuery":
        node_start_event = actual_ol_events[1]
        sql_query_start_event = actual_ol_events[2]

        assert node_start_event["run"]["runId"] == get_from_nullable_chain(
            sql_query_start_event, "run.facets.parent.run.runId".split(".")
        )

    elif dbt_event_type == "SQLQueryStatus":
        node_start_event = actual_ol_events[1]
        sql_query_start_event = actual_ol_events[2]
        sql_query_status_event = actual_ol_events[3]

        assert sql_query_start_event["run"]["runId"] == sql_query_status_event["run"]["runId"]
        assert node_start_event["run"]["runId"] == get_from_nullable_chain(
            sql_query_status_event, "run.facets.parent.run.runId".split(".")
        )


@pytest.mark.parametrize(
    "dbt_event, expected_job_name, dbt_command",
    [
        # model
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {"alias": "orders", "database": "postgres", "schema": "public"},
                        "resource_type": "model",
                        "unique_id": "model.jaffle_shop.orders",
                    }
                }
            },
            "model.jaffle_shop.orders",
            "run",
        ),
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {"alias": "orders", "database": "postgres", "schema": "public"},
                        "resource_type": "model",
                        "unique_id": "model.jaffle_shop.orders",
                    }
                }
            },
            "model.jaffle_shop.orders",
            "build",
        ),
        # snapshot
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {"alias": "orders", "database": "postgres", "schema": "public"},
                        "resource_type": "snapshot",
                        "unique_id": "snapshot.jaffle_shop.orders",
                    }
                }
            },
            "snapshot.jaffle_shop.orders",
            "snapshot",
        ),
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {"alias": "orders", "database": "postgres", "schema": "public"},
                        "resource_type": "snapshot",
                        "unique_id": "snapshot.jaffle_shop.orders",
                    }
                }
            },
            "snapshot.jaffle_shop.orders",
            "build",
        ),
        # test
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {
                            "alias": "not_null_customers_customer_id",
                            "database": "postgres",
                            "schema": "public_dbt_test__audit",
                        },
                        "resource_type": "test",
                        "unique_id": "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
                    }
                }
            },
            "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
            "test",
        ),
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {
                            "alias": "not_null_customers_customer_id",
                            "database": "postgres",
                            "schema": "public_dbt_test__audit",
                        },
                        "resource_type": "test",
                        "unique_id": "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
                    }
                }
            },
            "test.jaffle_shop.not_null_customers_customer_id.5c9bf9911d",
            "build",
        ),
        # seeds
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {
                            "alias": "raw_customers",
                            "database": "postgres",
                            "schema": "public",
                        },
                        "resource_type": "seed",
                        "unique_id": "seed.jaffle_shop.raw_customers",
                    }
                }
            },
            "seed.jaffle_shop.raw_customers",
            "seed",
        ),
        (
            {
                "data": {
                    "node_info": {
                        "node_relation": {
                            "alias": "raw_customers",
                            "database": "postgres",
                            "schema": "public",
                        },
                        "resource_type": "seed",
                        "unique_id": "seed.jaffle_shop.raw_customers",
                    }
                }
            },
            "seed.jaffle_shop.raw_customers",
            "build",
        ),
    ],
    ids=["run_mode", "build_model", "snapshot", "build_snapshot", "test", "build_test", "seed", "build_seed"],
)
def test_node_job_name(dbt_event, expected_job_name, dbt_command):
    """
    Tests the OL job name given to every dbt node type
    """
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )
    processor.dbt_command = dbt_command

    actual_job_name = processor._get_job_name(dbt_event)

    assert expected_job_name == actual_job_name


@pytest.mark.parametrize(
    "dbt_events, expected_sql_job_names",
    [
        (
            [
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_orders"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T19:45:53.117597Z"},
                },
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_orders"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T20:45:53.117597Z"},
                },
            ],
            [
                "model.jaffle_shop.stg_orders.sql.1",
                "model.jaffle_shop.stg_orders.sql.2",
            ],
        ),
        (
            [
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_payments"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T19:45:53.117597Z"},
                },
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_payments"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T20:45:53.117597Z"},
                },
                # the same as the first one
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_payments"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T19:45:53.117597Z"},
                },
            ],
            [
                "model.jaffle_shop.stg_payments.sql.1",
                "model.jaffle_shop.stg_payments.sql.2",
                "model.jaffle_shop.stg_payments.sql.1",
            ],
        ),
        (
            [
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_payments"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T19:45:53.117597Z"},
                },
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_payments"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T20:45:53.117597Z"},
                },
                # on a different node_id
                {
                    "data": {
                        "node_info": {"unique_id": "model.jaffle_shop.stg_orders"},
                        "sql": "SELECT * ...",
                    },
                    "info": {"ts": "2024-11-20T19:45:53.117597Z"},
                },
            ],
            [
                "model.jaffle_shop.stg_payments.sql.1",
                "model.jaffle_shop.stg_payments.sql.2",
                "model.jaffle_shop.stg_orders.sql.1",
            ],
        ),
    ],
    ids=["without_duplicates", "with_duplicate_sql", "with_different_node_ids"],
)
def test_sql_job_name(dbt_events, expected_sql_job_names):
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )

    actual_sql_job_names = [processor._get_sql_job_name(event) for event in dbt_events]

    assert actual_sql_job_names == expected_sql_job_names


@pytest.mark.parametrize(
    "dbt_log_events, expected_ol_events, parent_id_env_var",
    [
        (
            CURRENT_DIR + "/postgres/events/logs/successful_CommandCompleted.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_CommandCompleted_OL.yaml",
            f"my_parent_namespace/my_parent_job_name/{DUMMY_UUID_4}",
        ),
        (
            CURRENT_DIR + "/postgres/events/logs/successful_CommandCompleted.yaml",
            CURRENT_DIR + "/postgres/events/results/successful_CommandCompleted_OL.yaml",
            "",
        ),
    ],
    ids=["with_env_var", "without_env_var"],
)
def test_parent_run_metadata(dbt_log_events, expected_ol_events, parent_id_env_var, monkeypatch):
    monkeypatch.setenv("OPENLINEAGE_PARENT_ID", parent_id_env_var)
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command",
        lambda self: [json.dumps(dbt_event) for dbt_event in yaml.safe_load(open(dbt_log_events))],
    )

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )
    processor.manifest_path = CURRENT_DIR + "/postgres/run/target/manifest.json"
    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = yaml.safe_load(open(expected_ol_events))

    assert match(expected=expected_ol_events, result=actual_ol_events, ordered_list=True)


@mock.patch("datetime.datetime", wraps=datetime.datetime)
def test_missing_command_completed(mock_dt, monkeypatch):
    missing_command_completed = CURRENT_DIR + "/postgres/events/logs/missing_command_completed.yaml"
    missing_command_completed_ol_events = (
        CURRENT_DIR + "/postgres/events/results/missing_command_completed_OL.yaml"
    )
    mock_dt.now.return_value = datetime.datetime(2024, 1, 1, 0, 0, 0, 1)
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._run_dbt_command",
        lambda self: [json.dumps(dbt_event) for dbt_event in yaml.safe_load(open(missing_command_completed))],
    )

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )
    processor.manifest_path = CURRENT_DIR + "/postgres/run/target/manifest.json"
    actual_ol_events = list(ol_event_to_dict(event) for event in processor.parse())
    expected_ol_events = yaml.safe_load(open(missing_command_completed_ol_events))
    assert match(expected=expected_ol_events, result=actual_ol_events, ordered_list=True)


@pytest.mark.parametrize(
    "dbt_process_return_code, expected_processor_return_code",
    [(0, 0), (1, 1), (2, 2)],
    ids=["success", "failure", "error"],
)
def test_run_dbt_command(dbt_process_return_code, expected_processor_return_code, monkeypatch):
    popen_mock = mock.Mock()
    process_mock = mock.Mock()
    monkeypatch.setattr("openlineage.common.provider.dbt.structured_logs.subprocess.Popen", popen_mock)
    monkeypatch.setattr("openlineage.common.provider.dbt.structured_logs.IncrementalFileReader", mock.Mock())
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._open_dbt_log_file",
        mock.Mock(),
    )
    monkeypatch.setattr("os.stat", mock.Mock())
    popen_mock.return_value = process_mock
    process_mock.returncode = dbt_process_return_code
    process_mock.poll.return_value = 1

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run", "..."],
    )
    processor.manifest_path = CURRENT_DIR + "/postgres/run/target/manifest.json"
    processor.received_dbt_command_completed = True

    list(processor._run_dbt_command())

    assert expected_processor_return_code == processor.dbt_command_return_code


@pytest.mark.parametrize(
    "input_dbt_command_line, expected_dbt_command_line",
    [
        (
            [
                "dbt",
                "run",
                "--select",
                "orders",
                "--vars",
                "{'foo': 'bar'}",
                "--profiles-dir",
                "my_profiles_dir",
            ],
            [
                "dbt",
                "run",
                "--select",
                "orders",
                "--vars",
                "{'foo': 'bar'}",
                "--profiles-dir",
                "my_profiles_dir",
                "--log-format-file",
                "json",
                "--log-level-file",
                "debug",
                "--log-path",
                "./" + DUMMY_RANDOM_LOG_FILE,
                "--log-file-max-bytes",
                DBT_LOG_FILE_MAX_BYTES,
                "--write-json",
            ],
        ),
        (
            [
                "dbt",
                "run",
                "--select",
                "orders",
                "--vars",
                "{'foo': 'bar'}",
                "--profiles-dir",
                "my_profiles_dir",
                "--log-path",
                "dbt-logs-1234",
            ],
            [
                "dbt",
                "run",
                "--select",
                "orders",
                "--vars",
                "{'foo': 'bar'}",
                "--profiles-dir",
                "my_profiles_dir",
                "--log-path",
                "dbt-logs-1234",
                "--log-format-file",
                "json",
                "--log-level-file",
                "debug",
                "--log-file-max-bytes",
                DBT_LOG_FILE_MAX_BYTES,
                "--write-json",
            ],
        ),
    ],
    ids=["with_no_log_path", "with_log_path"],
)
def test_executed_dbt_command_line(input_dbt_command_line, expected_dbt_command_line, monkeypatch):
    popen_mock = mock.Mock()
    monkeypatch.setattr("openlineage.common.provider.dbt.structured_logs.subprocess.Popen", popen_mock)
    monkeypatch.setattr("openlineage.common.provider.dbt.structured_logs.IncrementalFileReader", mock.Mock())
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.structured_logs.DbtStructuredLogsProcessor._open_dbt_log_file",
        mock.Mock(),
    )
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.utils.generate_random_log_file_name", lambda: DUMMY_RANDOM_LOG_FILE
    )
    monkeypatch.setattr("os.stat", mock.Mock())

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=input_dbt_command_line,
    )
    processor.manifest_path = CURRENT_DIR + "/postgres/run/target/manifest.json"
    processor.received_dbt_command_completed = True

    list(processor._run_dbt_command())

    actual_command_line = popen_mock.call_args[0][0]
    assert actual_command_line == expected_dbt_command_line


@pytest.mark.parametrize(
    "input_dbt_command_line, expected_dbt_log_file_path",
    [
        (
            ["dbt", "run", "--select", "orders", "--project-dir", "my-dbt-project"],
            f"my-dbt-project/{DUMMY_RANDOM_LOG_FILE}/dbt.log",
        ),
        (
            [
                "dbt",
                "run",
                "--select",
                "orders",
                "--project-dir",
                "my-dbt-project",
                "--log-path",
                "dbt-logs-1234",
            ],
            "dbt-logs-1234/dbt.log",
        ),
        (
            ["dbt", "run", "--select", "orders", "--log-path", "dbt-logs-1234"],
            "dbt-logs-1234/dbt.log",
        ),
        (
            ["dbt", "run", "--select", "orders", "--log-path", "/tmp/dbt/dbt-logs-1234"],
            "/tmp/dbt/dbt-logs-1234/dbt.log",
        ),
    ],
    ids=["with_no_log_path", "with_log_path", "without_project_dir", "fully_qualified_path"],
)
def test_logfile_path(input_dbt_command_line, expected_dbt_log_file_path, monkeypatch):
    monkeypatch.setattr(
        "openlineage.common.provider.dbt.utils.generate_random_log_file_name", lambda: DUMMY_RANDOM_LOG_FILE
    )

    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=input_dbt_command_line,
    )
    processor.manifest_path = CURRENT_DIR + "/postgres/run/target/manifest.json"

    assert processor.dbt_log_file_path == expected_dbt_log_file_path


##################
# Manifest Integrity Tests
##################


@pytest.mark.parametrize(
    "manifest_data, expected_missing_nodes",
    [
        # Valid manifest - no missing nodes
        (
            {
                "parent_map": {
                    "model.test.orders": ["source.test.customers"],
                    "model.test.summary": ["model.test.orders"],
                },
                "nodes": {
                    "model.test.orders": {"name": "orders", "resource_type": "model"},
                    "model.test.summary": {"name": "summary", "resource_type": "model"},
                },
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            [],
        ),
        # Missing child node
        (
            {
                "parent_map": {"model.test.orders": ["model.test.missing_child"]},
                "nodes": {"model.test.orders": {"name": "orders", "resource_type": "model"}},
                "sources": {},
            },
            ["model.test.missing_child"],
        ),
        # Missing parent node
        (
            {
                "parent_map": {"model.test.missing_parent": ["source.test.customers"]},
                "nodes": {},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            ["model.test.missing_parent"],
        ),
        # Multiple missing nodes
        (
            {
                "parent_map": {
                    "model.test.missing1": ["model.test.missing2", "source.test.customers"],
                    "model.test.orders": ["model.test.missing3"],
                },
                "nodes": {"model.test.orders": {"name": "orders", "resource_type": "model"}},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            ["model.test.missing1", "model.test.missing2", "model.test.missing3"],
        ),
        # Mixed nodes and sources dependencies
        (
            {
                "parent_map": {
                    "model.test.orders": ["source.test.customers", "model.test.products"],
                    "model.test.summary": ["model.test.orders"],
                },
                "nodes": {
                    "model.test.orders": {"name": "orders", "resource_type": "model"},
                    "model.test.summary": {"name": "summary", "resource_type": "model"},
                    "model.test.products": {"name": "products", "resource_type": "model"},
                },
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            [],
        ),
    ],
    ids=[
        "valid_manifest",
        "missing_child_node",
        "missing_parent_node",
        "multiple_missing_nodes",
        "mixed_nodes_sources_valid",
    ],
)
def test_validate_manifest_integrity(manifest_data, expected_missing_nodes, caplog):
    """Test manifest integrity validation with various scenarios."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Set the compiled manifest directly to test validation
    processor._compiled_manifest = manifest_data

    # Clear any existing log records
    caplog.clear()

    # Call the validation method
    processor._validate_manifest_integrity()

    # Check warnings logged
    if expected_missing_nodes:
        assert "Manifest integrity check failed" in caplog.text
        warning_record = [
            record for record in caplog.records if "Manifest integrity check failed" in record.message
        ][0]
        assert f"Found {len(expected_missing_nodes)} nodes" in warning_record.message

        # Check that all expected missing nodes are mentioned in the log
        for missing_node in expected_missing_nodes:
            assert missing_node in warning_record.message
    else:
        assert "Manifest integrity check failed" not in caplog.text


def test_validate_manifest_integrity_empty_manifest(caplog):
    """Test validation with empty manifest."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Set empty manifest
    processor._compiled_manifest = {}

    # Clear any existing log records
    caplog.clear()

    # Call the validation method - should not crash
    processor._validate_manifest_integrity()

    # Should not log any warnings for empty manifest
    assert "Manifest integrity check failed" not in caplog.text


def test_validate_manifest_integrity_missing_sections(caplog):
    """Test validation with manifest missing key sections."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Manifest with missing nodes and sources sections
    processor._compiled_manifest = {
        "parent_map": {"model.test.orders": ["source.test.customers"]}
        # Missing "nodes" and "sources" sections
    }

    caplog.clear()

    # Call validation - should handle missing sections gracefully
    processor._validate_manifest_integrity()

    # Should detect missing nodes since nodes/sources sections are empty
    assert "Manifest integrity check failed" in caplog.text
    assert "model.test.orders" in caplog.text
    assert "source.test.customers" in caplog.text


def test_validate_manifest_integrity_large_number_missing_nodes(caplog):
    """Test log truncation when many nodes are missing."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Create manifest with many missing nodes (more than 10)
    missing_nodes = [f"model.test.missing_{i}" for i in range(15)]
    parent_map = {node: [] for node in missing_nodes}

    processor._compiled_manifest = {"parent_map": parent_map, "nodes": {}, "sources": {}}

    caplog.clear()

    processor._validate_manifest_integrity()

    # Should log warning with truncated list
    assert "Manifest integrity check failed" in caplog.text
    warning_record = [
        record for record in caplog.records if "Manifest integrity check failed" in record.message
    ][0]
    assert "Found 15 nodes" in warning_record.message

    # Should only show first 10 missing nodes in the log message
    logged_missing_nodes = [node for node in missing_nodes if node in warning_record.message]
    assert len(logged_missing_nodes) == 10


def test_manifest_integrity_called_during_loading(monkeypatch):
    """Test that _validate_manifest_integrity is called when manifest is loaded."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Mock the load_metadata method to return a test manifest
    test_manifest = {
        "parent_map": {"model.test.orders": ["source.test.customers"]},
        "nodes": {"model.test.orders": {}},
        "sources": {"source.test.customers": {}},
    }

    mock_load_metadata = mock.Mock(return_value=test_manifest)
    monkeypatch.setattr(processor, "load_metadata", mock_load_metadata)

    # Mock os.path.isfile to return True for manifest path
    monkeypatch.setattr("os.path.isfile", lambda path: True)

    # Mock the validation method to track if it's called
    mock_validate = mock.Mock()
    monkeypatch.setattr(processor, "_validate_manifest_integrity", mock_validate)

    # Access compiled_manifest property to trigger loading
    _ = processor.compiled_manifest

    # Verify validation was called
    mock_validate.assert_called_once()


def test_validate_manifest_integrity_no_parent_map(caplog):
    """Test validation when parent_map is missing from manifest."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Manifest without parent_map
    processor._compiled_manifest = {
        "nodes": {"model.test.orders": {}},
        "sources": {"source.test.customers": {}},
    }

    caplog.clear()

    # Should handle missing parent_map gracefully
    processor._validate_manifest_integrity()

    # Should not log any warnings when parent_map is missing
    assert "Manifest integrity check failed" not in caplog.text


##################
# Manifest Integrity Return Value Tests (Dense Checks)
##################


@pytest.mark.parametrize(
    "manifest_data, expected_result",
    [
        # Valid manifest - comprehensive scenario
        (
            {
                "parent_map": {
                    "model.test.orders": ["source.test.customers", "model.test.products"],
                    "model.test.summary": ["model.test.orders"],
                    "model.test.products": [],
                },
                "nodes": {
                    "model.test.orders": {"name": "orders", "resource_type": "model"},
                    "model.test.summary": {"name": "summary", "resource_type": "model"},
                    "model.test.products": {"name": "products", "resource_type": "model"},
                },
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            {
                "is_valid": True,
                "missing_nodes": [],
                "missing_parents": [],
                "missing_children": [],
                "total_missing": 0,
                "parent_map_size": 3,
                "available_nodes": 4,
            },
        ),
        # Missing child only
        (
            {
                "parent_map": {"model.test.orders": ["model.test.missing_child", "source.test.customers"]},
                "nodes": {"model.test.orders": {"name": "orders", "resource_type": "model"}},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            {
                "is_valid": False,
                "missing_nodes": ["model.test.missing_child"],
                "missing_parents": [],
                "missing_children": ["model.test.missing_child"],
                "total_missing": 1,
                "parent_map_size": 1,
                "available_nodes": 2,
            },
        ),
        # Missing parent only
        (
            {
                "parent_map": {"model.test.missing_parent": ["source.test.customers"]},
                "nodes": {},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            {
                "is_valid": False,
                "missing_nodes": ["model.test.missing_parent"],
                "missing_parents": ["model.test.missing_parent"],
                "missing_children": [],
                "total_missing": 1,
                "parent_map_size": 1,
                "available_nodes": 1,
            },
        ),
        # Complex missing scenario - multiple parents and children
        (
            {
                "parent_map": {
                    "model.test.missing_parent1": ["model.test.missing_child1", "source.test.customers"],
                    "model.test.orders": ["model.test.missing_child2", "model.test.missing_child1"],
                    "model.test.missing_parent2": ["source.test.customers"],
                },
                "nodes": {"model.test.orders": {"name": "orders", "resource_type": "model"}},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            {
                "is_valid": False,
                "missing_nodes": [
                    "model.test.missing_parent1",
                    "model.test.missing_child1",
                    "model.test.missing_child2",
                    "model.test.missing_parent2",
                ],
                "missing_parents": ["model.test.missing_parent1", "model.test.missing_parent2"],
                "missing_children": ["model.test.missing_child1", "model.test.missing_child2"],
                "total_missing": 4,
                "parent_map_size": 3,
                "available_nodes": 2,
            },
        ),
        # Duplicate missing nodes (same node referenced multiple times)
        (
            {
                "parent_map": {
                    "model.test.orders": ["model.test.missing_duplicate"],
                    "model.test.summary": ["model.test.missing_duplicate", "model.test.orders"],
                },
                "nodes": {
                    "model.test.orders": {"name": "orders", "resource_type": "model"},
                    "model.test.summary": {"name": "summary", "resource_type": "model"},
                },
                "sources": {},
            },
            {
                "is_valid": False,
                "missing_nodes": ["model.test.missing_duplicate"],  # Should be deduplicated
                "missing_parents": [],
                "missing_children": ["model.test.missing_duplicate"],
                "total_missing": 1,
                "parent_map_size": 2,
                "available_nodes": 2,
            },
        ),
        # Empty parent_map
        (
            {
                "parent_map": {},
                "nodes": {"model.test.orders": {"name": "orders", "resource_type": "model"}},
                "sources": {"source.test.customers": {"name": "customers", "resource_type": "source"}},
            },
            {
                "is_valid": True,
                "missing_nodes": [],
                "missing_parents": [],
                "missing_children": [],
                "total_missing": 0,
                "parent_map_size": 0,
                "available_nodes": 2,
            },
        ),
        # Node with empty dependencies
        (
            {
                "parent_map": {
                    "model.test.orders": [],  # Empty dependencies
                    "model.test.summary": ["model.test.orders"],
                },
                "nodes": {
                    "model.test.orders": {"name": "orders", "resource_type": "model"},
                    "model.test.summary": {"name": "summary", "resource_type": "model"},
                },
                "sources": {},
            },
            {
                "is_valid": True,
                "missing_nodes": [],
                "missing_parents": [],
                "missing_children": [],
                "total_missing": 0,
                "parent_map_size": 2,
                "available_nodes": 2,
            },
        ),
    ],
    ids=[
        "valid_comprehensive",
        "missing_child_only",
        "missing_parent_only",
        "complex_multiple_missing",
        "duplicate_missing_nodes",
        "empty_parent_map",
        "empty_dependencies",
    ],
)
def test_manifest_integrity_return_values(manifest_data, expected_result):
    """Test manifest integrity validation return values with dense verification."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    processor._compiled_manifest = manifest_data

    result = processor._validate_manifest_integrity()

    # Verify result is ManifestIntegrityResult
    assert isinstance(result, ManifestIntegrityResult)

    # Dense verification of each field
    assert result.is_valid == expected_result["is_valid"], "is_valid mismatch"
    assert result.missing_nodes == expected_result["missing_nodes"], "missing_nodes mismatch"
    assert result.missing_parents == expected_result["missing_parents"], "missing_parents mismatch"
    assert result.missing_children == expected_result["missing_children"], "missing_children mismatch"
    assert result.total_missing == expected_result["total_missing"], "total_missing mismatch"
    assert result.parent_map_size == expected_result["parent_map_size"], "parent_map_size mismatch"
    assert result.available_nodes == expected_result["available_nodes"], "available_nodes mismatch"

    # Consistency checks
    assert result.total_missing == len(result.missing_nodes), (
        "total_missing should equal length of missing_nodes"
    )
    assert result.is_valid == (result.total_missing == 0), "is_valid should be True iff total_missing is 0"
    assert len(set(result.missing_parents) & set(result.missing_children)) == 0, (
        "A node shouldn't be both missing parent and missing child"
    )
    assert set(result.missing_parents + result.missing_children) <= set(result.missing_nodes), (
        "All missing parents/children should be in missing_nodes"
    )


def test_manifest_integrity_edge_cases():
    """Test edge cases with empty and malformed manifests."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Test empty manifest
    processor._compiled_manifest = {}
    result = processor._validate_manifest_integrity()
    expected_empty = ManifestIntegrityResult(
        is_valid=True,
        missing_nodes=[],
        missing_parents=[],
        missing_children=[],
        total_missing=0,
        parent_map_size=0,
        available_nodes=0,
    )
    assert result == expected_empty

    # Test None manifest
    processor._compiled_manifest = None
    result = processor._validate_manifest_integrity()
    assert result == expected_empty

    # Test manifest missing sections
    processor._compiled_manifest = {"other_section": {}}
    result = processor._validate_manifest_integrity()
    assert result == expected_empty


def test_manifest_integrity_deduplication():
    """Test that duplicate missing nodes are properly deduplicated while preserving order."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Manifest where same missing node appears multiple times
    processor._compiled_manifest = {
        "parent_map": {
            "model.test.missing1": ["model.test.missing_dup"],  # First occurrence
            "model.test.missing2": ["model.test.missing_dup"],  # Second occurrence
            "model.test.missing1": [  # noqa: F601
                "model.test.missing_dup"
            ],  # Third occurrence (same parent as first)
        },
        "nodes": {},
        "sources": {},
    }

    result = processor._validate_manifest_integrity()

    # Should have deduplicated missing nodes but preserved order of first occurrence
    assert result.missing_nodes == ["model.test.missing1", "model.test.missing_dup", "model.test.missing2"]
    assert result.missing_parents == ["model.test.missing1", "model.test.missing2"]
    assert result.missing_children == ["model.test.missing_dup"]
    assert result.total_missing == 3
    assert not result.is_valid


def test_manifest_integrity_large_scale():
    """Test manifest integrity with large numbers of nodes."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    # Create large manifest with many nodes
    num_valid_nodes = 100
    num_missing_nodes = 50

    valid_nodes = {f"model.test.valid_{i}": {} for i in range(num_valid_nodes)}
    valid_sources = {f"source.test.valid_{i}": {} for i in range(20)}

    parent_map = {}
    # Add some valid dependencies
    for i in range(num_valid_nodes // 2):
        parent_map[f"model.test.valid_{i}"] = (
            [f"model.test.valid_{i + 50}"] if i + 50 < num_valid_nodes else []
        )

    # Add missing dependencies
    for i in range(num_missing_nodes):
        parent_map[f"model.test.missing_{i}"] = [f"model.test.missing_child_{i}"]

    processor._compiled_manifest = {"parent_map": parent_map, "nodes": valid_nodes, "sources": valid_sources}

    result = processor._validate_manifest_integrity()

    # Verify large scale handling
    assert result.parent_map_size == num_valid_nodes // 2 + num_missing_nodes
    assert result.available_nodes == num_valid_nodes + 20  # nodes + sources
    assert result.total_missing == num_missing_nodes * 2  # parent + child for each missing
    assert not result.is_valid
    assert len(result.missing_nodes) == num_missing_nodes * 2
    assert len(result.missing_parents) == num_missing_nodes
    assert len(result.missing_children) == num_missing_nodes


def test_manifest_integrity_types_validation():
    """Test that return values have correct types."""
    processor = DbtStructuredLogsProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=CURRENT_DIR,
        target="postgres",
        dbt_command_line=["dbt", "run"],
    )

    processor._compiled_manifest = {
        "parent_map": {"model.test.missing": ["model.test.missing_child"]},
        "nodes": {},
        "sources": {},
    }

    result = processor._validate_manifest_integrity()

    # Type validation
    assert isinstance(result, ManifestIntegrityResult)
    assert isinstance(result.is_valid, bool)
    assert isinstance(result.missing_nodes, list)
    assert isinstance(result.missing_parents, list)
    assert isinstance(result.missing_children, list)
    assert isinstance(result.total_missing, int)
    assert isinstance(result.parent_map_size, int)
    assert isinstance(result.available_nodes, int)

    # All list elements should be strings
    for node in result.missing_nodes + result.missing_parents + result.missing_children:
        assert isinstance(node, str)

    # All counts should be non-negative
    assert result.total_missing >= 0
    assert result.parent_map_size >= 0
    assert result.available_nodes >= 0
