# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import importlib.machinery
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from collections import defaultdict
from enum import Enum
from pathlib import Path
from unittest import mock

import attr
import pytest
from openlineage.common.provider.dbt.otel_processor import DbtOtelProcessor, run_dbt_untouched
from openlineage.common.provider.dbt.otel_records import nanos_to_iso, parse_record
from openlineage.common.provider.dbt.utils import detect_dbt_engine

HERE = Path(__file__).absolute().parent
PROJECT_DIR = str(HERE / "project")
FIXTURES = HERE / "fixtures"
BUILD_COMMAND = ["dbt", "build", "--profiles-dir", PROJECT_DIR]
TRACE_ID = "0123456789abcdef0123456789abcdef"


def as_dict(event) -> dict:
    return attr.asdict(event, value_serializer=lambda _, __, v: v.value if isinstance(v, Enum) else v)


def fixture_lines(name: str = "build.jsonl") -> list[str]:
    return (FIXTURES / name).read_text().splitlines()


def load_manifest(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def make_processor(command=None, **kwargs) -> DbtOtelProcessor:
    return DbtOtelProcessor(
        producer="https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/dbt",
        job_namespace="dbt-test-namespace",
        project_dir=PROJECT_DIR,
        dbt_command_line=list(command or BUILD_COMMAND),
        dbt_version="2.0.8",
        **kwargs,
    )


def fake_parse(on_wait):
    """A stand-in for the `dbt parse` process: wait() returns on_wait(process, timeout), until killed."""
    parses = []

    class Parse:
        def __init__(self, cmd, env, stdout, stderr):
            self.cmd, self.env, self.killed = cmd, env, False
            parses.append(self)

        def wait(self, timeout=None):
            return -9 if self.killed else on_wait(self, timeout)

        def kill(self):
            self.killed = True

    return Parse, parses


def replay(processor, lines, return_code=1, manifest="parse_manifest.json") -> list[dict]:
    """Run the processor over recorded telemetry instead of a dbt process.

    The pre-parse loads ``manifest``: a fixture name, a manifest, or None for a pre-parse that failed.
    """

    def pre_parse():
        if manifest is not None:
            processor._compiled_manifest = load_manifest(manifest) if isinstance(manifest, str) else manifest

    def run_dbt():
        yield from lines
        processor.dbt_command_return_code = return_code

    with (
        mock.patch.object(processor, "_run_pre_parse", pre_parse),
        mock.patch.object(processor, "_run_dbt_command", run_dbt),
    ):
        return [as_dict(event) for event in processor.parse()]


def by_job(events: list[dict]) -> dict[str, list[dict]]:
    jobs = defaultdict(list)
    for event in events:
        jobs[event["job"]["name"]].append(event)
    return jobs


def dataset_names(datasets: list[dict]) -> list[str]:
    return sorted(d["name"].rsplit(".", 1)[-1] for d in datasets)


def record(record_type, name, **fields) -> str:
    """One telemetry line shaped like dbt 2.0.8 output."""
    line = {
        "record_type": record_type,
        "event_type": f"v1.public.events.fusion.x.{name}",
        "trace_id": fields.pop("trace_id", TRACE_ID),
        "span_id": fields.pop("span_id", None),
        "parent_span_id": None,
        "attributes": fields.pop("attributes", {}),
        "start_time_unix_nano": 1791833392710809493,
        "end_time_unix_nano": 1791833393710809493,
        "time_unix_nano": 1791833392910809493,
    }
    line.update(fields)
    return json.dumps(line)


def invocation(trace_id=TRACE_ID, raw_command="dbt build"):
    return [
        record(
            "SpanStart",
            "Invocation",
            trace_id=trace_id,
            span_id="inv",
            attributes={"invocation_id": trace_id, "raw_command": raw_command},
        ),
    ]


def invocation_end(trace_id=TRACE_ID, code=1, message=None):
    return record(
        "SpanEnd", "Invocation", trace_id=trace_id, span_id="inv", status={"code": code, "message": message}
    )


@pytest.fixture(autouse=True)
def no_ambient_dbt_env(monkeypatch):
    for name in (
        "DBT_OTEL_FILE_NAME",
        "DBT_ENGINE_OTEL_FILE_NAME",
        "DBT_LOG_PATH",
        "DBT_ENGINE_LOG_PATH",
        "DBT_INVOCATION_ID",
        "DBT_ENGINE_INVOCATION_ID",
        "OPENLINEAGE_DBT_TEMP_DIR",
    ):
        monkeypatch.delenv(name, raising=False)


##########################
# build on DuckDB, dbt 2.0.8
##########################


@pytest.fixture(scope="module")
def build_jobs():
    return by_job(replay(make_processor(), fixture_lines()))


@pytest.mark.parametrize(
    "job, event_types, job_type, inputs, outputs",
    [
        ("seed.otel_jaffle.raw_orders", ["START", "COMPLETE"], "SEED", [], ["raw_orders"]),
        ("model.otel_jaffle.stg_orders", ["START", "COMPLETE"], "MODEL", ["raw_orders"], ["stg_orders"]),
        # an ephemeral model is a no-op skip in v2 telemetry and writes no relation
        ("model.otel_jaffle.eph_order_totals", ["START", "COMPLETE"], "MODEL", ["stg_orders"], []),
        (
            "model.otel_jaffle.customer_totals",
            ["START", "COMPLETE"],
            "MODEL",
            ["eph_order_totals", "stg_customers"],
            ["customer_totals"],
        ),
        ("model.otel_jaffle.broken_model", ["START", "FAIL"], "MODEL", [], ["broken_model"]),
        (
            "model.otel_jaffle.downstream_of_broken",
            ["START", "ABORT"],
            "MODEL",
            ["broken_model"],
            ["downstream_of_broken"],
        ),
        # skipped because its unit test fails
        (
            "model.otel_jaffle.active_customers",
            ["START", "ABORT"],
            "MODEL",
            ["stg_customers"],
            ["active_customers"],
        ),
        (
            "snapshot.otel_jaffle.snap_customers",
            ["START", "COMPLETE"],
            "SNAPSHOT",
            ["stg_customers"],
            ["snap_customers"],
        ),
    ],
)
def test_node_events(build_jobs, job, event_types, job_type, inputs, outputs):
    events = build_jobs[job]
    assert [e["eventType"] for e in events] == event_types
    terminal = events[-1]
    assert terminal["job"]["facets"]["jobType"]["jobType"] == job_type
    assert dataset_names(terminal["inputs"]) == inputs
    assert dataset_names(terminal["outputs"]) == outputs
    assert "extractionError" not in terminal["run"]["facets"]


def test_wrapper_events(build_jobs):
    events = build_jobs["dbt-run-otel_jaffle"]
    assert [e["eventType"] for e in events] == ["START", "FAIL"]
    assert events[1]["run"]["facets"]["errorMessage"]["message"] == "Executed with errors"
    facets = events[0]["run"]["facets"]
    assert facets["processing_engine"]["version"] == "2.0.8"
    invocation_start = next(
        json.loads(line) for line in fixture_lines() if '"SpanStart"' in line and ".Invocation" in line
    )
    assert facets["dbt_run"]["invocation_id"] == invocation_start["attributes"]["invocation_id"]


def test_node_runs_are_children_of_the_wrapper(build_jobs):
    wrapper_run_id = build_jobs["dbt-run-otel_jaffle"][0]["run"]["runId"]
    stg_orders = build_jobs["model.otel_jaffle.stg_orders"][0]
    assert stg_orders["run"]["facets"]["parent"]["run"]["runId"] == wrapper_run_id


def test_model_error_message(build_jobs):
    message = build_jobs["model.otel_jaffle.broken_model"][-1]["run"]["facets"]["errorMessage"]["message"]
    assert "does_not_exist" in message


def test_model_carries_compiled_sql_and_column_lineage(build_jobs):
    terminal = build_jobs["model.otel_jaffle.stg_orders"][-1]
    assert "raw_orders" in terminal["job"]["facets"]["sql"]["query"]
    assert "columnLineage" in terminal["outputs"][0]["facets"]
    # no row counts on dbt Core v2: it reports rows_affected = 0 for every model build
    assert not terminal["outputs"][0]["outputFacets"]


def _assertion(events):
    return events[-1]["inputs"][0]["facets"]["dataQualityAssertions"]["assertions"][0]


def test_passing_test(build_jobs):
    events = build_jobs["test.otel_jaffle.unique_stg_customers_customer_id.c7614daada"]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    assertion = _assertion(events)
    assert (assertion["assertion"], assertion["success"], assertion["actual"]) == ("unique", True, "0")


def test_failing_test_gets_dbt_v1_message(build_jobs):
    job = next(
        j for j in build_jobs if j.startswith("test.otel_jaffle.accepted_values_order_statuses_status")
    )
    events = build_jobs[job]
    assert [e["eventType"] for e in events] == ["START", "FAIL"]
    assert (
        events[-1]["run"]["facets"]["errorMessage"]["message"] == "Got 1 result, configured to fail if != 0"
    )
    assertion = _assertion(events)
    assert (assertion["success"], assertion["actual"]) == (False, "1")


def test_warning_test_completes_with_failed_assertion(build_jobs):
    job = next(j for j in build_jobs if j.startswith("test.otel_jaffle.accepted_values_stg_customers_status"))
    events = build_jobs[job]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    assert "errorMessage" not in events[-1]["run"]["facets"]
    assert _assertion(events)["success"] is False


def test_singular_test(build_jobs):
    events = build_jobs["test.otel_jaffle.assert_positive_amounts"]
    tests = events[-1]["run"]["facets"]["test"]["tests"]
    assert (tests[0]["type"], tests[0]["status"], tests[0]["actual"]) == ("singular", "pass", "0")


def test_unit_tests(build_jobs):
    passing = build_jobs["unit_test.otel_jaffle.stg_customers.ut_stg_customers_renames"]
    failing = build_jobs["unit_test.otel_jaffle.active_customers.ut_active_customers_wrong_expectation"]
    assert [e["eventType"] for e in passing] == ["START", "COMPLETE"]
    assert [e["eventType"] for e in failing] == ["START", "FAIL"]
    for event in passing + failing:
        assert event["job"]["facets"]["jobType"]["jobType"] == "UNIT_TEST"
        assert event["inputs"] == [] and event["outputs"] == []
        assert "extractionError" not in event["run"]["facets"]
    assert "differ" in failing[-1]["run"]["facets"]["errorMessage"]["message"]


def test_sql_child_events(build_jobs):
    node_run_id = build_jobs["model.otel_jaffle.stg_orders"][0]["run"]["runId"]
    sql_jobs = sorted(j for j in build_jobs if j.startswith("model.otel_jaffle.stg_orders.sql."))
    assert sql_jobs[0] == "model.otel_jaffle.stg_orders.sql.1"
    for job in sql_jobs:
        events = build_jobs[job]
        assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
        assert events[0]["job"]["facets"]["jobType"]["jobType"] == "SQL"
        assert events[0]["run"]["facets"]["parent"]["run"]["runId"] == node_run_id
    failed = [j for j in build_jobs if j.startswith("model.otel_jaffle.broken_model.sql.")]
    assert [e["eventType"] for e in build_jobs[failed[-1]]] == ["START", "FAIL"]


def test_skip_parse_holds_final_events_for_the_end_of_run_manifest():
    processor = make_processor(skip_parse=True)
    processor.manifest_path = str(FIXTURES / "end_of_run_manifest.json")
    events = replay(processor, fixture_lines())
    starts = {e["job"]["name"]: i for i, e in enumerate(events) if e["eventType"] == "START"}
    finals = {e["job"]["name"]: (i, e) for i, e in enumerate(events) if e["eventType"] != "START"}
    model = "model.otel_jaffle.customer_totals"
    assert events[starts[model]]["inputs"] == []
    index, final = finals[model]
    assert dataset_names(final["inputs"]) == ["eph_order_totals", "stg_customers"]
    # node final events wait for the manifest dbt writes after the RUN phase
    last_node_start = max(i for job, i in starts.items() if job.startswith("model."))
    assert index > last_node_start


def test_skip_parse_without_end_of_run_manifest_still_closes_runs():
    processor = make_processor(skip_parse=True)
    processor.manifest_path = "/nonexistent/manifest.json"
    jobs = by_job(replay(processor, fixture_lines()))
    events = jobs["model.otel_jaffle.customer_totals"]
    assert [e["eventType"] for e in events] == ["START", "COMPLETE"]
    assert events[-1]["inputs"] == []
    assert dataset_names(events[-1]["outputs"]) == ["customer_totals"]


def test_killed_dbt_aborts_open_runs():
    lines = fixture_lines()
    query_start = next(
        i
        for i, line in enumerate(lines)
        if '"SpanStart"' in line and "QueryExecuted" in line and "model.otel_jaffle.customer_totals" in line
    )
    jobs = by_job(replay(make_processor(), lines[: query_start + 1], return_code=-15))
    assert [e["eventType"] for e in jobs["model.otel_jaffle.customer_totals"]] == ["START", "ABORT"]
    assert [e["eventType"] for e in jobs["model.otel_jaffle.customer_totals.sql.1"]] == ["START", "ABORT"]
    assert [e["eventType"] for e in jobs["dbt-run-otel_jaffle"]] == ["START", "ABORT"]


BROKEN_TEST = "test.otel_jaffle.not_null_stg_customers_customer_id.e2cfb1f9aa"


def _with_broken_test_end(lines: list[str]) -> list[str]:
    """The lines with the RUN-phase end of BROKEN_TEST made unprocessable."""
    corrupted = []
    for line in lines:
        data = json.loads(line)
        attributes = data.get("attributes") or {}
        if (
            data["record_type"] == "SpanEnd"
            and attributes.get("unique_id") == BROKEN_TEST
            and attributes.get("phase") == "EXECUTION_PHASE_RUN"
        ):
            attributes["node_test_detail"] = {"test_outcome": "TEST_OUTCOME_PASSED", "failing_rows": "n/a"}
            line = json.dumps(data)
        corrupted.append(line)
    return corrupted


def _event_types(events: list[dict]) -> dict[str, list[str]]:
    return {job: [e["eventType"] for e in job_events] for job, job_events in by_job(events).items()}


def test_a_record_that_fails_to_process_costs_only_its_own_node(caplog):
    lines = fixture_lines()
    expected = _event_types(replay(make_processor(), lines))
    with caplog.at_level(logging.WARNING):
        events = replay(make_processor(), _with_broken_test_end(lines))
    expected[BROKEN_TEST] = ["START", "ABORT"]
    assert _event_types(events) == expected
    assert "OpenLineage failed to process 1 dbt telemetry record" in caplog.text


@pytest.mark.parametrize("end_of_run_manifest", [True, False], ids=["manifest-record", "at-exit"])
def test_a_held_node_end_that_fails_costs_only_its_own_node(end_of_run_manifest, caplog):
    # With --ol-skip-parse every node end is held, and they're all released together.
    lines = fixture_lines()
    if not end_of_run_manifest:
        lines = [line for line in lines if "ArtifactWritten" not in line]

    def held_replay(lines):
        processor = make_processor(skip_parse=True)
        processor.manifest_path = str(FIXTURES / "end_of_run_manifest.json")
        return replay(processor, lines, manifest=None)

    expected = _event_types(held_replay(lines))
    with caplog.at_level(logging.WARNING):
        events = held_replay(_with_broken_test_end(lines))
    expected[BROKEN_TEST] = ["START", "ABORT"]
    assert _event_types(events) == expected
    assert "OpenLineage failed to process 1 dbt telemetry record" in caplog.text


def test_unread_invocation_end_with_exit_code_zero_completes_the_wrapper():
    lines = [
        line
        for line in fixture_lines()
        if not ('"SpanEnd"' in line and "fusion.invocation.Invocation" in line)
    ]
    assert len(lines) == len(fixture_lines()) - 1
    jobs = by_job(replay(make_processor(), lines, return_code=0))
    assert [e["eventType"] for e in jobs["dbt-run-otel_jaffle"]] == ["START", "COMPLETE"]


##########################
# adapter shapes (Snowflake, Databricks)
##########################


def _node_lines(unique_id="model.shop.orders", queries=(), error_log=None, outcome="NODE_OUTCOME_ERROR"):
    lines = [
        record(
            "SpanStart",
            "NodeProcessed",
            span_id="np",
            attributes={
                "unique_id": unique_id,
                "database": "PROD",
                "schema": "ANALYTICS",
                "identifier": "orders",
                "materialization": "NODE_MATERIALIZATION_TABLE",
            },
        ),
        record(
            "SpanStart",
            "NodeEvaluated",
            span_id="ne",
            attributes={"unique_id": unique_id, "phase": "EXECUTION_PHASE_RUN"},
        ),
    ]
    for span_id, end_attributes, status in queries:
        lines.append(
            record(
                "SpanStart",
                "QueryExecuted",
                span_id=span_id,
                attributes={"unique_id": unique_id, "phase": "EXECUTION_PHASE_RUN", "sql": "select 1"},
            )
        )
        lines.append(
            record(
                "SpanEnd",
                "QueryExecuted",
                span_id=span_id,
                status=status,
                attributes={"unique_id": unique_id, "phase": "EXECUTION_PHASE_RUN", **end_attributes},
            )
        )
    if error_log:
        lines.append(
            record(
                "LogRecord",
                "LogMessage",
                body=error_log,
                attributes={"unique_id": unique_id, "original_severity_text": "ERROR", "code": 1308},
            )
        )
    lines.append(
        record(
            "SpanEnd",
            "NodeEvaluated",
            span_id="ne",
            status={"code": 2, "message": "error"},
            attributes={"unique_id": unique_id, "phase": "EXECUTION_PHASE_RUN", "node_outcome": outcome},
        )
    )
    return lines


SHOP_MANIFEST = {
    "metadata": {},
    "nodes": {
        "model.shop.orders": {
            "database": "PROD",
            "schema": "ANALYTICS",
            "alias": "orders",
            "unique_id": "model.shop.orders",
            "columns": {},
            "config": {},
        }
    },
    "sources": {},
    "parent_map": {"model.shop.orders": []},
}


def test_snowflake_query_id_and_adapter_error():
    queries = [
        (
            "q1",
            {"query_outcome": "QUERY_OUTCOME_SUCCESS", "query_id": "01c7afd1-0818-fcb0-0006-f74b0fa685d6"},
            {"code": 1},
        ),
        (
            "q2",
            {
                "query_outcome": "QUERY_OUTCOME_ERROR",
                "query_error_vendor_code": 2003,
                "query_error_adapter_message": "NotFound: [Snowflake] 002003 (42S02): Object does not exist",
            },
            {"code": 2, "message": "NotFound"},
        ),
    ]
    lines = invocation() + _node_lines(queries=queries) + [invocation_end(code=2)]
    jobs = by_job(replay(make_processor(), lines, manifest=SHOP_MANIFEST))
    first = jobs["model.shop.orders.sql.1"][-1]
    assert (
        first["run"]["facets"]["externalQuery"]["externalQueryId"] == "01c7afd1-0818-fcb0-0006-f74b0fa685d6"
    )
    second = jobs["model.shop.orders.sql.2"][-1]
    assert second["eventType"] == "FAIL"
    assert "002003" in second["run"]["facets"]["errorMessage"]["message"]
    # with no error log record, the node falls back to its failed query's message
    node = jobs["model.shop.orders"][-1]
    assert node["eventType"] == "FAIL"
    assert "002003" in node["run"]["facets"]["errorMessage"]["message"]


def test_databricks_error_detail_comes_from_the_error_log_record():
    queries = [
        (
            "q1",
            {"query_outcome": "QUERY_OUTCOME_ERROR", "query_error_vendor_code": -2147483648},
            {"code": 2, "message": None},
        )
    ]
    body = "Database Error in model orders\n  [DIVIDE_BY_ZERO] Division by zero. SQLSTATE: 22012"
    lines = invocation() + _node_lines(queries=queries, error_log=body) + [invocation_end(code=2)]
    jobs = by_job(replay(make_processor(), lines, manifest=SHOP_MANIFEST))
    assert "errorMessage" not in jobs["model.shop.orders.sql.1"][-1]["run"]["facets"]
    assert jobs["model.shop.orders"][-1]["run"]["facets"]["errorMessage"]["message"] == body


def test_user_otel_file_run_is_found_whatever_command_dbt_records():
    # A wrapper script on PATH that adds flags changes the command line dbt records.
    lines = (
        invocation(raw_command="/opt/dbt/bin/dbt build --profiles-dir /etc/dbt")
        + _node_lines(outcome="NODE_OUTCOME_SUCCESS")
        + [invocation_end(code=1)]
    )
    processor = make_processor()
    with mock.patch.object(processor, "_resolve_otel_file", lambda: ("/var/log/dbt/otel.jsonl", False)):
        jobs = by_job(replay(processor, lines, return_code=0, manifest=None))
    assert [e["eventType"] for e in jobs["model.shop.orders"]] == ["START", "COMPLETE"]
    assert jobs["dbt-run-otel_jaffle"][0]["run"]["facets"]["dbt_run"]["invocation_id"] == TRACE_ID


def test_user_otel_file_without_telemetry_warns(caplog):
    processor = make_processor()
    with mock.patch.object(processor, "_resolve_otel_file", lambda: ("/var/log/dbt/otel.jsonl", False)):
        assert replay(processor, [], return_code=0, manifest=None) == []
    assert "Found no telemetry for this dbt run in /var/log/dbt/otel.jsonl" in caplog.text


def test_wrapper_records_why_node_events_could_lack_inputs():
    processor = make_processor(skip_parse=True)
    processor.manifest_path = str(FIXTURES / "end_of_run_manifest.json")
    jobs = by_job(replay(processor, fixture_lines(), manifest=None))
    for event in jobs["dbt-run-otel_jaffle"]:
        dbt_run = event["run"]["facets"]["dbt_run"]
        assert (dbt_run["pre_parse"], dbt_run["write_json"]) == ("skipped", True)
    # Node events don't repeat them.
    assert jobs["model.otel_jaffle.customer_totals"][0]["run"]["facets"]["dbt_run"]["pre_parse"] is None


def test_no_telemetry_and_failing_dbt_reports_a_failed_wrapper():
    jobs = by_job(replay(make_processor(), [], return_code=2, manifest=None))
    events = jobs["dbt-run-otel_jaffle"]
    assert [e["eventType"] for e in events] == ["START", "FAIL"]
    assert events[1]["run"]["facets"]["errorMessage"]["message"] == "dbt exited with code 2"


##########################
# launching dbt
##########################


def test_pre_parse_command(monkeypatch, tmp_path):
    monkeypatch.setenv("DBT_OTEL_FILE_NAME", "user-otel.jsonl")
    monkeypatch.setenv("DBT_INVOCATION_ID", "01a13197-c645-7823-b6f6-f0d0ac9644ea")
    command = [
        *BUILD_COMMAND,
        "-t",
        "prod",
        "--vars",
        "{x: 1}",
        "--select",
        "orders",
        "--target-path",
        "elsewhere",
    ]
    processor = make_processor(command=command)
    processor.temp_dir = str(tmp_path)

    def write_manifest(parse, timeout):
        assert timeout == 600
        target = Path(parse.cmd[parse.cmd.index("--target-path") + 1])
        target.mkdir()
        (target / "manifest.json").write_text((FIXTURES / "parse_manifest.json").read_text())
        return 0

    popen, parses = fake_parse(write_manifest)
    with mock.patch("openlineage.common.provider.dbt.otel_processor.subprocess.Popen", popen):
        processor._run_pre_parse()

    cmd, env = parses[0].cmd, parses[0].env
    assert cmd[:6] == [
        "dbt",
        "parse",
        "--target-path",
        str(tmp_path / "target"),
        "--log-path",
        str(tmp_path / "logs"),
    ]
    assert cmd[6:] == ["--profiles-dir", PROJECT_DIR, "--target", "prod", "--vars", "{x: 1}"]
    assert env["DBT_DISABLE_VERSION_CHECK"] == "1"
    assert "DBT_OTEL_FILE_NAME" not in env and "DBT_INVOCATION_ID" not in env
    assert "model.otel_jaffle.customer_totals" in processor.compiled_manifest["nodes"]
    assert processor._pre_parse == "succeeded"


def test_failed_pre_parse_leaves_no_manifest(tmp_path, caplog):
    processor = make_processor()
    processor.temp_dir = str(tmp_path)
    popen, _ = fake_parse(lambda parse, timeout: 1)
    with mock.patch("openlineage.common.provider.dbt.otel_processor.subprocess.Popen", popen):
        processor._run_pre_parse()
    assert processor.compiled_manifest == {}
    assert processor._pre_parse == "failed"
    assert "`dbt parse` before the run failed" in caplog.text


def test_hung_pre_parse_is_stopped_and_the_run_goes_ahead(tmp_path, caplog):
    processor = make_processor()
    processor.temp_dir = str(tmp_path)

    def hang(parse, timeout):
        raise subprocess.TimeoutExpired(parse.cmd, timeout)

    popen, parses = fake_parse(hang)
    with mock.patch("openlineage.common.provider.dbt.otel_processor.subprocess.Popen", popen):
        processor._run_pre_parse()
    assert parses[0].killed
    assert processor.compiled_manifest == {}
    assert processor._pre_parse == "timed_out"
    assert "took longer than 600 s and was stopped" in caplog.text


@pytest.mark.parametrize(
    "stop, return_code",
    [
        (lambda: (_ for _ in ()).throw(KeyboardInterrupt), 130),  # Ctrl-C
        (lambda: os.kill(os.getpid(), signal.SIGTERM), 143),
    ],
    ids=["sigint", "sigterm"],
)
def test_stop_during_pre_parse_kills_it_cleans_up_and_skips_dbt(monkeypatch, tmp_path, stop, return_code):
    monkeypatch.setenv("OPENLINEAGE_DBT_TEMP_DIR", str(tmp_path))
    processor = make_processor()

    def stopped(parse, timeout):
        stop()
        time.sleep(1)  # a SIGTERM's handler runs here
        return 0

    popen, parses = fake_parse(stopped)
    previous = signal.getsignal(signal.SIGTERM)
    with (
        mock.patch("openlineage.common.provider.dbt.otel_processor.subprocess.Popen", popen),
        mock.patch.object(processor, "_run_dbt_command") as run_dbt,
    ):
        assert list(processor.parse()) == []
    assert parses[0].killed
    assert not run_dbt.called
    assert processor.dbt_command_return_code == return_code
    assert list(tmp_path.iterdir()) == []
    assert signal.getsignal(signal.SIGTERM) is previous


def test_temp_dir_is_removed(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENLINEAGE_DBT_TEMP_DIR", str(tmp_path))
    processor = make_processor()
    seen = []
    original_prepare = processor._prepare

    def prepare():
        original_prepare()
        seen.append(processor.temp_dir)

    with mock.patch.object(processor, "_prepare", prepare):
        replay(processor, fixture_lines())
    assert seen[0].startswith(str(tmp_path / "openlineage-dbt-"))
    assert not os.path.exists(seen[0])
    assert processor.temp_dir is None


def test_otel_file_owned_by_dbt_ol(tmp_path):
    processor = make_processor()
    processor.temp_dir = str(tmp_path)
    assert processor._resolve_otel_file() == (str(tmp_path / "otel.jsonl"), True)


def test_otel_file_chosen_by_the_user(monkeypatch, tmp_path):
    processor = make_processor(
        command=[*BUILD_COMMAND, "--otel-file-name", "otel.jsonl", "--log-path", str(tmp_path)]
    )
    assert processor._resolve_otel_file() == (str(tmp_path / "otel.jsonl"), False)
    monkeypatch.setenv("DBT_OTEL_FILE_NAME", str(tmp_path / "abs.jsonl"))
    assert make_processor()._resolve_otel_file() == (str(tmp_path / "abs.jsonl"), False)
    # dbt 2.0.8 writes to DBT_ENGINE_OTEL_FILE_NAME when both are set.
    monkeypatch.setenv("DBT_ENGINE_OTEL_FILE_NAME", str(tmp_path / "engine.jsonl"))
    assert make_processor()._resolve_otel_file() == (str(tmp_path / "engine.jsonl"), False)


@pytest.mark.parametrize(
    "log_paths, used",
    [
        ({"DBT_LOG_PATH": "legacy"}, "legacy"),
        ({"DBT_ENGINE_LOG_PATH": "engine"}, "engine"),
        # dbt 2.0.8 writes to DBT_ENGINE_LOG_PATH when both are set.
        ({"DBT_LOG_PATH": "legacy", "DBT_ENGINE_LOG_PATH": "engine"}, "engine"),
    ],
)
def test_relative_otel_file_resolves_against_the_log_path_dbt_uses(monkeypatch, tmp_path, log_paths, used):
    monkeypatch.setenv("DBT_ENGINE_OTEL_FILE_NAME", "otel.jsonl")
    for name, directory in log_paths.items():
        monkeypatch.setenv(name, str(tmp_path / directory))
    assert make_processor()._resolve_otel_file() == (str(tmp_path / used / "otel.jsonl"), False)
    flag = make_processor(command=[*BUILD_COMMAND, "--log-path", str(tmp_path / "flag")])
    assert flag._resolve_otel_file() == (str(tmp_path / "flag" / "otel.jsonl"), False)


def test_first_interrupt_waits_for_dbt_and_sigterm_is_passed_on():
    processor = make_processor()
    process = mock.Mock()
    with processor._forward_signals(process):
        os.kill(os.getpid(), signal.SIGINT)
        time.sleep(0.2)
        assert not process.kill.called  # dbt got the Ctrl-C itself and stops gracefully
        os.kill(os.getpid(), signal.SIGTERM)
        time.sleep(0.2)
        process.send_signal.assert_called_once_with(signal.SIGTERM)


def test_second_interrupt_kills_dbt_and_stops_without_waiting(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENLINEAGE_DBT_TEMP_DIR", str(tmp_path))
    started = tmp_path / "started"
    fake_dbt = tmp_path / "dbt"
    fake_dbt.write_text(
        f"#!{sys.executable}\n"
        "import pathlib, signal, time\n"
        "signal.signal(signal.SIGINT, signal.SIG_IGN)\n"  # a dbt that hangs on its way out
        f"pathlib.Path({str(started)!r}).touch()\n"
        "time.sleep(60)\n"
    )
    fake_dbt.chmod(0o755)
    processor = make_processor(command=[str(fake_dbt), *BUILD_COMMAND[1:]], skip_parse=True)
    default_handler = signal.getsignal(signal.SIGINT)

    def press_ctrl_c_twice():
        while not started.exists() or signal.getsignal(signal.SIGINT) is default_handler:
            time.sleep(0.01)
        for _ in range(2):
            os.kill(os.getpid(), signal.SIGINT)
            time.sleep(0.2)

    threading.Thread(target=press_ctrl_c_twice, daemon=True).start()
    began = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        list(processor.parse())
    assert time.monotonic() - began < 10
    assert processor.dbt_command_return_code == -signal.SIGKILL
    assert not list(tmp_path.glob("openlineage-dbt-*"))
    assert signal.getsignal(signal.SIGINT) is default_handler


def test_run_dbt_untouched_reports_a_missing_dbt_like_a_shell(tmp_path, caplog):
    assert run_dbt_untouched([str(tmp_path / "missing" / "dbt"), "init"], logging.getLogger("t")) == 127
    assert "Could not run" in caplog.text


def test_dbt_that_cannot_start_fails_the_run(tmp_path):
    missing_dbt = str(tmp_path / "missing" / "dbt")
    processor = make_processor(command=[missing_dbt, *BUILD_COMMAND[1:]])
    with mock.patch.object(processor, "_run_pre_parse", lambda: None):
        jobs = by_job([as_dict(e) for e in processor.parse()])
    assert processor.dbt_command_return_code == 127
    events = jobs["dbt-run-otel_jaffle"]
    assert [e["eventType"] for e in events] == ["START", "FAIL"]
    assert events[1]["run"]["facets"]["errorMessage"]["message"] == "dbt exited with code 127"


def test_dbt_still_runs_when_preparing_fails(tmp_path):
    (tmp_path / "profiles.yml").write_text("other_profile: {}\n")
    processor = make_processor(command=["dbt", "build", "--profiles-dir", str(tmp_path)])
    with mock.patch("openlineage.common.provider.dbt.otel_processor.subprocess.Popen") as popen:
        popen.return_value.__enter__.return_value.wait.return_value = 0
        assert list(processor.parse()) == []
    assert processor.dbt_command_return_code == 0
    assert popen.called


##########################
# records and engine detection
##########################


def test_parse_record():
    parsed = parse_record(fixture_lines()[0])
    assert parsed is not None and parsed.trace_id
    assert parse_record("not json") is None
    assert parse_record('{"no": "record_type"}') is None


def test_nanos_to_iso():
    assert nanos_to_iso(1791833392710809493) == "2026-10-12T19:29:52.710809+00:00"


def _fake_dbt(tmp_path, content: bytes) -> str:
    path = tmp_path / "dbt"
    path.write_bytes(content)
    path.chmod(0o755)
    return str(path)


def test_detect_engine_native_binary(monkeypatch, tmp_path):
    path = _fake_dbt(tmp_path, b"\x7fELF\x02\x01\x01" + b"\x00" * 64)
    with mock.patch("openlineage.common.provider.dbt.utils.shutil.which", return_value=path):
        assert detect_dbt_engine() == (2, None)


@pytest.mark.parametrize(
    "script, interpreter",
    [
        (b"#!/opt/venv/bin/python\nfrom dbt.cli.main import cli\n", ["/opt/venv/bin/python"]),
        (b"#!/usr/bin/env python3\nfrom dbt.cli.main import cli\n", ["/usr/bin/env", "python3"]),
        (
            b"#!/bin/sh\n'''exec' \"/very/long/venv/bin/python\" \"$0\" \"$@\"\n' '''\n",
            ["/very/long/venv/bin/python"],
        ),
    ],
)
@pytest.mark.parametrize("probe_output, expected", [("2 2.0.8\n", (2, "2.0.8")), ("1\n", (1, None))])
def test_detect_engine_python_script(monkeypatch, tmp_path, script, interpreter, probe_output, expected):
    path = _fake_dbt(tmp_path, script)
    with (
        mock.patch("openlineage.common.provider.dbt.utils.shutil.which", return_value=path),
        mock.patch(
            "openlineage.common.provider.dbt.utils.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, stdout=probe_output),
        ) as run,
    ):
        assert detect_dbt_engine() == expected
    assert run.call_args.args[0][: len(interpreter)] == interpreter


@pytest.mark.parametrize(
    "output, expected",
    [("dbt 2.0.8\n", (2, "2.0.8")), ("Core:\n  - installed: 1.11.2\n", (1, None))],
)
def test_detect_engine_falls_back_to_version_output(monkeypatch, tmp_path, output, expected):
    path = _fake_dbt(tmp_path, b"MZ\x90\x00")  # neither a native Unix binary nor a script
    with (
        mock.patch("openlineage.common.provider.dbt.utils.shutil.which", return_value=path),
        mock.patch(
            "openlineage.common.provider.dbt.utils.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, stdout=output),
        ),
    ):
        assert detect_dbt_engine() == expected


@pytest.mark.parametrize(
    "script",
    [
        b'#!/usr/bin/env bash\nexec pyenv exec dbt "$@"\n',  # pyenv and asdf shims
        b'#!/bin/bash\nexec /opt/dbt/bin/dbt "$@"\n',
        b"#!/usr/bin/env -S node --no-warnings\n",
    ],
)
def test_detect_engine_never_runs_the_probe_with_a_non_python_interpreter(monkeypatch, tmp_path, script):
    path = _fake_dbt(tmp_path, script)
    with (
        mock.patch("openlineage.common.provider.dbt.utils.shutil.which", return_value=path),
        mock.patch(
            "openlineage.common.provider.dbt.utils.subprocess.run",
            return_value=subprocess.CompletedProcess([], 0, stdout="dbt 2.0.8\n"),
        ) as run,
    ):
        assert detect_dbt_engine() == (2, "2.0.8")
    run.assert_called_once()
    assert run.call_args.args[0] == [path, "--version"]
    assert "env" not in run.call_args.kwargs


def _dbt_in_this_environment(monkeypatch, tmp_path, *, importable=True, with_core=True):
    """A `dbt` console script in this environment's scripts directory, and the dbt package it runs."""
    scripts = tmp_path / "bin"
    scripts.mkdir()
    path = _fake_dbt(scripts, b"#!/opt/venv/bin/python\nfrom dbt.cli.main import cli\n")
    package = tmp_path / "site-packages" / "dbt"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("raise AssertionError('dbt must not be imported')\n")
    if with_core:
        (package / f"_core{importlib.machinery.EXTENSION_SUFFIXES[0]}").write_bytes(b"")
    spec = importlib.machinery.ModuleSpec("dbt", None, is_package=True)
    spec.submodule_search_locations = [str(package)]
    utils = "openlineage.common.provider.dbt.utils"
    monkeypatch.setattr(f"{utils}.shutil.which", lambda _: path)
    monkeypatch.setattr(f"{utils}.sysconfig.get_path", lambda name: str(scripts))
    monkeypatch.setattr(f"{utils}.importlib.util.find_spec", lambda name: spec if importable else None)
    monkeypatch.setattr(f"{utils}.importlib.metadata.version", lambda name: "2.0.8")


@pytest.mark.parametrize("with_core, expected", [(True, (2, "2.0.8")), (False, (1, None))])
def test_detect_engine_reads_a_dbt_in_this_environment_without_a_process(
    monkeypatch, tmp_path, with_core, expected
):
    _dbt_in_this_environment(monkeypatch, tmp_path, with_core=with_core)
    with mock.patch("openlineage.common.provider.dbt.utils.subprocess.run", side_effect=AssertionError):
        assert detect_dbt_engine() == expected


def test_detect_engine_inspects_dbt_when_this_environment_cannot_import_it(monkeypatch, tmp_path):
    _dbt_in_this_environment(monkeypatch, tmp_path, importable=False)
    with mock.patch(
        "openlineage.common.provider.dbt.utils.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout="2 2.0.8\n"),
    ) as run:
        assert detect_dbt_engine() == (2, "2.0.8")
    assert run.call_args.args[0][0] == "/opt/venv/bin/python"


def test_detect_engine_without_dbt(monkeypatch):
    with mock.patch("openlineage.common.provider.dbt.utils.shutil.which", return_value=None):
        assert detect_dbt_engine() == (1, None)
