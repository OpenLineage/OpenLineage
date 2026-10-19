# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""Records of the OpenTelemetry JSONL file dbt Core v2 writes (``--otel-file-name`` / ``DBT_OTEL_FILE_NAME``).

Each line is one span start, span end or log record. Event types are named
``v1.public.events.fusion.<package>.<Name>``; only the last segment is kept here.
"""

import json
from datetime import datetime, timezone
from typing import Any

import attr

SPAN_START = "SpanStart"
SPAN_END = "SpanEnd"
LOG_RECORD = "LogRecord"

INVOCATION = "Invocation"
PROCESS = "Process"
NODE_PROCESSED = "NodeProcessed"
NODE_EVALUATED = "NodeEvaluated"
QUERY_EXECUTED = "QueryExecuted"
COMPILED_CODE = "CompiledCode"
LOG_MESSAGE = "LogMessage"
ARTIFACT_WRITTEN = "ArtifactWritten"

PHASE_RUN = "EXECUTION_PHASE_RUN"

NODE_OUTCOME_SUCCESS = "NODE_OUTCOME_SUCCESS"
NODE_OUTCOME_ERROR = "NODE_OUTCOME_ERROR"
NODE_OUTCOME_SKIPPED = "NODE_OUTCOME_SKIPPED"
NODE_OUTCOME_CANCELED = "NODE_OUTCOME_CANCELED"
NODE_SKIP_REASON_NO_OP = "NODE_SKIP_REASON_NO_OP"
NODE_MATERIALIZATION_EPHEMERAL = "NODE_MATERIALIZATION_EPHEMERAL"

TEST_OUTCOME_PASSED = "TEST_OUTCOME_PASSED"
TEST_OUTCOME_WARNED = "TEST_OUTCOME_WARNED"
TEST_OUTCOME_FAILED = "TEST_OUTCOME_FAILED"

QUERY_OUTCOME_SUCCESS = "QUERY_OUTCOME_SUCCESS"
ARTIFACT_TYPE_MANIFEST = "ARTIFACT_TYPE_MANIFEST"

# Span status codes follow OpenTelemetry: 0 unset, 1 ok, 2 error.
STATUS_CODE_ERROR = 2


@attr.define
class OtelRecord:
    record_type: str
    name: str
    trace_id: str | None
    span_id: str | None
    attributes: dict[str, Any]
    start_time_unix_nano: int | None = None
    end_time_unix_nano: int | None = None
    time_unix_nano: int | None = None
    status: dict[str, Any] | None = None
    body: str | None = None

    @property
    def unique_id(self) -> str | None:
        return self.attributes.get("unique_id")

    @property
    def phase(self) -> str | None:
        return self.attributes.get("phase")

    @property
    def is_error_log(self) -> bool:
        return (
            self.record_type == LOG_RECORD
            and self.name == LOG_MESSAGE
            and self.attributes.get("original_severity_text") == "ERROR"
        )

    @property
    def status_code(self) -> int | None:
        return (self.status or {}).get("code")

    @property
    def status_message(self) -> str | None:
        return (self.status or {}).get("message")

    def event_time(self) -> str:
        """When the record happened: the start of a span start, the end of a span end, a log record's time."""
        if self.record_type == SPAN_END:
            return nanos_to_iso(self.end_time_unix_nano)
        if self.record_type == SPAN_START:
            return nanos_to_iso(self.start_time_unix_nano)
        return nanos_to_iso(self.time_unix_nano)


def nanos_to_iso(nanos: int | None) -> str:
    if nanos is None:
        return datetime.now(timezone.utc).isoformat()
    seconds, remainder = divmod(int(nanos), 1_000_000_000)
    return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(microsecond=remainder // 1000).isoformat()


def parse_record(line: str) -> OtelRecord | None:
    """Parse one JSONL line; None for anything that isn't a telemetry record."""
    try:
        raw = json.loads(line)
    except ValueError:
        return None
    if not isinstance(raw, dict) or "record_type" not in raw:
        return None
    event_type = raw.get("event_type") or ""
    return OtelRecord(
        record_type=raw["record_type"],
        name=event_type.rsplit(".", 1)[-1],
        trace_id=raw.get("trace_id"),
        span_id=raw.get("span_id"),
        attributes=raw.get("attributes") or {},
        start_time_unix_nano=raw.get("start_time_unix_nano"),
        end_time_unix_nano=raw.get("end_time_unix_nano"),
        time_unix_nano=raw.get("time_unix_nano"),
        status=raw.get("status"),
        body=raw.get("body"),
    )
