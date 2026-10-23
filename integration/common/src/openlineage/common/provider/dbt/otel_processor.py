# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
"""OpenLineage events for dbt Core v2, read live from the OpenTelemetry output dbt writes while it runs.

dbt Core v2 writes ``manifest.json`` only after every node has run, and its telemetry names the relation
each node writes but not the ones it reads. So before the real command, the processor runs ``dbt parse``
into a temporary directory and takes node inputs from that manifest. ``--ol-skip-parse`` skips the parse;
final node events then wait for the manifest dbt writes at the end of the run.
"""

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Generator, Iterator

import attr
from openlineage.client.event_v2 import InputDataset, OutputDataset, RunEvent, RunState
from openlineage.client.facet_v2 import (
    error_message_run,
    external_query_run,
    job_type_job,
    sql_job,
)
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import DbtNodeJobFacet, ParentRunMetadata
from openlineage.common.provider.dbt.live_events import DbtLiveProcessor
from openlineage.common.provider.dbt.otel_records import (
    ARTIFACT_TYPE_MANIFEST,
    ARTIFACT_WRITTEN,
    COMPILED_CODE,
    INVOCATION,
    NODE_EVALUATED,
    NODE_MATERIALIZATION_EPHEMERAL,
    NODE_OUTCOME_CANCELED,
    NODE_OUTCOME_ERROR,
    NODE_OUTCOME_SKIPPED,
    NODE_OUTCOME_SUCCESS,
    NODE_PROCESSED,
    NODE_SKIP_REASON_NO_OP,
    PHASE_RUN,
    PROCESS,
    QUERY_EXECUTED,
    QUERY_OUTCOME_SUCCESS,
    SPAN_END,
    SPAN_START,
    STATUS_CODE_ERROR,
    TEST_OUTCOME_FAILED,
    TEST_OUTCOME_WARNED,
    OtelRecord,
    nanos_to_iso,
    parse_record,
)
from openlineage.common.provider.dbt.processor import ModelNode
from openlineage.common.provider.dbt.utils import (
    TEMP_DIR_ENV,
    generate_run_event,
    get_dbt_profiles_dir,
    job_type_for_unique_id,
)
from openlineage.common.utils import IncrementalFileReader, parse_single_arg

OTEL_FILE_ENV = "DBT_OTEL_FILE_NAME"
# dbt Core v2 reads each DBT_ENGINE_<X> environment variable as DBT_<X>, and prefers it when both are set.
_OTEL_FILE_ENVS = ("DBT_ENGINE_OTEL_FILE_NAME", OTEL_FILE_ENV)
_LOG_PATH_ENVS = ("DBT_ENGINE_LOG_PATH", "DBT_LOG_PATH")
_INVOCATION_ID_ENVS = ("DBT_ENGINE_INVOCATION_ID", "DBT_INVOCATION_ID")
# A parse of a 10k-model project takes about 20 s; past this, the run goes ahead without it.
_PRE_PARSE_TIMEOUT_S = 600
# Exit codes a shell reports for a process stopped by SIGINT and SIGTERM.
_EXIT_INTERRUPTED = 128 + signal.SIGINT
_EXIT_TERMINATED = 128 + signal.SIGTERM
# Flags that change what the project compiles to, copied from the dbt command onto the pre-parse.
_PARSE_FLAGS = (
    (["--project-dir"], "--project-dir"),
    (["--profiles-dir"], "--profiles-dir"),
    (["--profile"], "--profile"),
    (["-t", "--target"], "--target"),
    (["--vars"], "--vars"),
)
_POLL_INTERVAL_S = 0.05
# Empty polls after dbt exits before giving up on a missing Invocation end record.
_DRAIN_POLLS = 20


@attr.define
class _NodeState:
    unique_id: str
    database: str | None = None
    schema: str | None = None
    identifier: str | None = None
    materialization: str | None = None
    compiled_sql: str | None = None
    started: bool = False
    query_count: int = 0
    # Text of the node's ERROR log record; the only error detail some adapters (Databricks) report.
    error_message: str | None = None
    failed_query_message: str | None = None


@attr.define
class _QueryState:
    unique_id: str
    start_event: RunEvent


class _TerminatedBeforeRun(Exception):
    """SIGTERM arrived while dbt-ol was preparing, before dbt started."""


def run_dbt_untouched(dbt_command_line: list[str], logger) -> int:
    """Run dbt without OpenLineage and return its exit code; 127 or 126, like a shell, if it can't start."""
    try:
        with subprocess.Popen(dbt_command_line, stdout=sys.stdout, stderr=sys.stderr) as process:
            return process.wait()
    except OSError as e:
        logger.error("Could not run %s: %s", dbt_command_line[0], e)
        return 127 if isinstance(e, FileNotFoundError) else 126


def _pluralize(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


class DbtOtelProcessor(DbtLiveProcessor):
    def __init__(
        self,
        dbt_command_line: list[str],
        *args,
        skip_parse: bool = False,
        dbt_version: str | None = None,
        **kwargs,
    ):
        super().__init__(dbt_command_line, *args, **kwargs)
        self.project_dir = os.path.abspath(kwargs.get("project_dir") or ".")
        self.profiles_dir = get_dbt_profiles_dir(command=self.dbt_command_line)
        self.skip_parse = skip_parse
        self._dbt_version = dbt_version

        self.temp_dir: str | None = None
        self.otel_file_path: str | None = None
        # False when the user chose the OTel file: it's read in place and never deleted. Only one dbt
        # invocation writes to it at a time.
        self.owns_otel_file = True
        self._dbt_started_at: float | None = None
        # How the pre-parse went: succeeded, failed, timed_out or skipped.
        self._pre_parse: str | None = None
        # Whether dbt writes manifest.json at the end of the run, from its Invocation record.
        self._write_json: bool | None = None
        self._failed_records = 0

        self._trace_id: str | None = None
        self._nodes: dict[str, _NodeState] = {}
        self._queries: dict[str, _QueryState] = {}
        # RUN-phase node end records waiting for the end-of-run manifest
        self._held_node_ends: list[OtelRecord] = []
        self._wrapper_start: RunEvent | None = None
        self._invocation_end: OtelRecord | None = None

    # ---------------------------------------------------------------- run

    def parse(self) -> Generator[RunEvent, None, None]:  # type: ignore[override]
        """Run dbt and yield OpenLineage events while it runs.

        dbt always runs: when preparing fails, it runs untouched. Only a Ctrl-C or SIGTERM during the
        preparation stops dbt-ol before dbt starts.
        """
        try:
            try:
                with self._stop_on_sigterm():
                    self._prepare()
            except (KeyboardInterrupt, _TerminatedBeforeRun) as e:
                # The user or a scheduler stopped dbt-ol during the pre-parse: dbt must not start after it.
                interrupted = isinstance(e, KeyboardInterrupt)
                self.logger.warning(
                    "dbt-ol was %s before dbt started; not running dbt",
                    "interrupted" if interrupted else "terminated",
                )
                self.dbt_command_return_code = _EXIT_INTERRUPTED if interrupted else _EXIT_TERMINATED
                return
            except Exception:
                self.logger.exception(
                    "OpenLineage could not prepare for this dbt run; running dbt without OpenLineage events"
                )
                self._remove_temp_dir()
                self.dbt_command_return_code = run_dbt_untouched(self.dbt_command_line, self.logger)
                return

            lines = self._run_dbt_command()
            try:
                for line in lines:
                    # A record that fails is skipped, so it costs only its own node's events.
                    try:
                        yield from self._handle_line(line)
                    except Exception:
                        self._skip_failed_record()
            except Exception:
                self.logger.exception(
                    "OpenLineage failed to process dbt telemetry. This does not make dbt execution fail, "
                    "however, data might not end up in your configured lineage backend."
                )
            finally:
                # Waits for dbt to exit and records its exit code, also after a processing error.
                lines.close()
            yield from self._finalize()
            if self._failed_records:
                self.logger.warning(
                    "OpenLineage failed to process %s",
                    _pluralize(self._failed_records, "dbt telemetry record"),
                )
        finally:
            self._remove_temp_dir()

    def _skip_failed_record(self) -> None:
        """Log a telemetry record that failed to process, from inside its except block."""
        self._failed_records += 1
        if self._failed_records == 1:
            self.logger.exception(
                "OpenLineage failed to process a dbt telemetry record and skipped it; some events might be "
                "missing. This does not make dbt execution fail."
            )
        else:
            self.logger.debug("OpenLineage failed to process a dbt telemetry record", exc_info=True)

    def _prepare(self) -> None:
        self.extract_adapter_type(self.profile)
        self.extract_dataset_namespace(self.profile)
        self.temp_dir = tempfile.mkdtemp(prefix="openlineage-dbt-", dir=os.environ.get(TEMP_DIR_ENV) or None)
        self.logger.info("dbt-ol temporary directory: %s", self.temp_dir)
        self.otel_file_path, self.owns_otel_file = self._resolve_otel_file()
        if self.skip_parse:
            self._pre_parse = "skipped"
        else:
            self._run_pre_parse()

    def _remove_temp_dir(self) -> None:
        if self.temp_dir:
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            self.temp_dir = None

    def _resolve_otel_file(self) -> tuple[str, bool]:
        """The OTel file to read, and whether dbt-ol owns it.

        A file the user configured is read in place; dbt resolves a relative name against the log path.
        """
        args = self.dbt_command_line[1:]
        configured = parse_single_arg(args, ["--otel-file-name"]) or next(
            (os.environ[name] for name in _OTEL_FILE_ENVS if os.environ.get(name)), None
        )
        if configured:
            if not os.path.isabs(configured):
                log_path = parse_single_arg(args, ["--log-path"]) or next(
                    (os.environ[name] for name in _LOG_PATH_ENVS if os.environ.get(name)), None
                )
                project_dir = parse_single_arg(args, ["--project-dir"]) or "."
                configured = os.path.join(log_path or os.path.join(project_dir, "logs"), configured)
            return os.path.abspath(configured), False
        assert self.temp_dir is not None
        return os.path.join(self.temp_dir, "otel.jsonl"), True

    def _run_pre_parse(self) -> None:
        """Run `dbt parse` into the temporary directory and load its manifest.

        The parse never writes the user's target/ (that would break `--state target/`), never appends to
        their telemetry file, and doesn't reuse a fixed invocation id. A failed parse is logged; final node
        events then wait for the end-of-run manifest instead.
        """
        assert self.temp_dir is not None
        target_path = os.path.join(self.temp_dir, "target")
        command = [
            self.dbt_command_line[0],
            "parse",
            "--target-path",
            target_path,
            "--log-path",
            os.path.join(self.temp_dir, "logs"),
        ]
        for names, flag in _PARSE_FLAGS:
            value = parse_single_arg(self.dbt_command_line[1:], names)
            if value is not None:
                command += [flag, value]
        # dbt Core v2 checks online for a newer release before a command unless this is set.
        env = {**os.environ, "DBT_DISABLE_VERSION_CHECK": "1"}
        for name in _OTEL_FILE_ENVS + _INVOCATION_ID_ENVS:
            env.pop(name, None)

        output_path = os.path.join(self.temp_dir, "parse-output.log")
        started = time.time()
        with open(output_path, "w", encoding="utf-8") as output:
            process = subprocess.Popen(command, env=env, stdout=output, stderr=subprocess.STDOUT)
            try:
                return_code = process.wait(timeout=_PRE_PARSE_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                self._pre_parse = "timed_out"
                self.logger.warning(
                    "`dbt parse` before the run took longer than %d s and was stopped; node inputs will come "
                    "from the manifest dbt writes at the end of the run.",
                    _PRE_PARSE_TIMEOUT_S,
                )
                return
            except BaseException:
                # Ctrl-C or SIGTERM: don't leave the parse running on its own.
                process.kill()
                process.wait()
                raise
        if return_code != 0:
            self._pre_parse = "failed"
            with open(output_path, encoding="utf-8", errors="replace") as output:
                tail = output.read()[-2000:]
            self.logger.warning(
                "`dbt parse` before the run failed (exit code %d); node inputs will come from the manifest "
                "dbt writes at the end of the run. Output:\n%s",
                return_code,
                tail,
            )
            return
        self._compiled_manifest = self.load_metadata(
            os.path.join(target_path, "manifest.json"), list(range(2, 13)), self.logger
        )
        self._pre_parse = "succeeded"
        self.logger.info("Parsed the dbt project in %.1f s", time.time() - started)

    def _run_dbt_command(self) -> Generator[str, None, None]:
        """Run dbt with telemetry going to the OTel file and yield the file's lines as dbt writes them."""
        assert self.otel_file_path is not None
        env = dict(os.environ)
        if self.owns_otel_file:
            env[OTEL_FILE_ENV] = self.otel_file_path
        # dbt appends to the file; start reading where this run starts.
        start_offset = os.path.getsize(self.otel_file_path) if os.path.exists(self.otel_file_path) else 0

        self._dbt_started_at = time.time()
        try:
            process = subprocess.Popen(self.dbt_command_line, env=env, stdout=sys.stdout, stderr=sys.stderr)
        except OSError as e:
            # Exit like a shell would: 127 when dbt isn't found, 126 when it can't be executed.
            self.dbt_command_return_code = 127 if isinstance(e, FileNotFoundError) else 126
            raise
        try:
            with self._forward_signals(process):
                yield from self._tail(process, start_offset)
        except Exception:
            self.logger.exception(
                "An exception occurred in OpenLineage dbt wrapper. "
                "It should not affect underlying dbt execution that is still running."
            )
            process.wait()
        finally:
            if process.poll() is None:
                process.wait()
            self.dbt_command_return_code = process.returncode if process.returncode is not None else -1

    def _tail(self, process: subprocess.Popen, start_offset: int) -> Iterator[str]:
        assert self.otel_file_path is not None
        otel_file = None
        empty_polls_after_exit = 0
        try:
            while True:
                exited = process.poll() is not None
                if otel_file is None and os.path.exists(self.otel_file_path):
                    otel_file = open(self.otel_file_path, encoding="utf-8")  # noqa: SIM115
                    otel_file.seek(start_offset)
                    reader = IncrementalFileReader(otel_file)
                read_any = False
                if otel_file is not None:
                    for line in reader.read_lines(sys.maxsize):
                        read_any = True
                        yield line
                if exited and not read_any:
                    empty_polls_after_exit += 1
                    if self._invocation_end is not None or empty_polls_after_exit >= _DRAIN_POLLS:
                        return
                if not read_any:
                    time.sleep(_POLL_INTERVAL_S)
        finally:
            if otel_file is not None:
                otel_file.close()

    @contextlib.contextmanager
    def _stop_on_sigterm(self) -> Iterator[None]:
        """Until dbt starts, make SIGTERM unwind the preparation, so its cleanup runs."""
        if threading.current_thread() is not threading.main_thread():
            yield
            return

        def stop(signum, _frame):
            raise _TerminatedBeforeRun(f"signal {signum}")

        previous = signal.signal(signal.SIGTERM, stop)
        try:
            yield
        finally:
            signal.signal(signal.SIGTERM, previous)

    @contextlib.contextmanager
    def _forward_signals(self, process: subprocess.Popen) -> Iterator[None]:
        """Keep reading until dbt exits after SIGINT or SIGTERM, so the runs still open can be closed.

        SIGTERM is passed on to dbt. SIGINT is not: a Ctrl-C in a terminal reaches dbt through the shared
        process group, and passing it on would make dbt see it twice. So a `kill -INT` sent to dbt-ol alone
        doesn't reach dbt. A second SIGINT kills dbt and stops dbt-ol without waiting; the runs still open
        then get no final event.
        """
        if threading.current_thread() is not threading.main_thread():
            yield
            return

        interrupts = 0

        def forward(signum, _frame):
            nonlocal interrupts
            if signum == signal.SIGINT:
                interrupts += 1
                if interrupts == 1:
                    self.logger.info(
                        "Interrupted; waiting for dbt to exit. Press Ctrl-C again to stop without waiting"
                    )
                    return
                self.logger.warning("Interrupted again; killing dbt")
                # Killed first, so waiting for dbt on the way out doesn't block again.
                with contextlib.suppress(ProcessLookupError):
                    process.kill()
                raise KeyboardInterrupt
            self.logger.info("Received signal %d; waiting for dbt to exit", signum)
            with contextlib.suppress(ProcessLookupError):
                process.send_signal(signum)

        previous = {sig: signal.signal(sig, forward) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            yield
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    # ---------------------------------------------------------------- records

    def _handle_line(self, line: str) -> Iterator[RunEvent]:
        record = parse_record(line)
        if record is None:
            return
        if self._trace_id is None:
            if record.record_type == SPAN_START and record.name == PROCESS:
                self._dbt_version = self._dbt_version or record.attributes.get("version")
            elif record.record_type == SPAN_START and record.name == INVOCATION:
                # Reading starts where this run starts, so the first invocation is this run's.
                self._trace_id = record.trace_id
                eval_args = record.attributes.get("eval_args")
                if isinstance(eval_args, dict):
                    self._write_json = eval_args.get("write_json")
                yield self._wrapper_start_event(
                    record.event_time(), record.attributes.get("invocation_id") or record.trace_id
                )
            return
        if record.trace_id != self._trace_id:
            return

        if record.name == NODE_PROCESSED and record.record_type == SPAN_START:
            self._remember_node_identity(record)
        elif record.name == COMPILED_CODE and record.unique_id:
            self._node(record.unique_id).compiled_sql = record.attributes.get("sql")
        elif record.name == NODE_EVALUATED and record.phase == PHASE_RUN and self._emits_events(record):
            if record.record_type == SPAN_START:
                yield self._node_start_event(record)
            elif record.record_type == SPAN_END:
                yield from self._on_node_end(record)
        elif record.name == QUERY_EXECUTED and record.unique_id and job_type_for_unique_id(record.unique_id):
            if record.record_type == SPAN_START:
                yield self._query_start_event(record)
            elif record.record_type == SPAN_END and record.span_id in self._queries:
                yield self._query_end_event(record)
        elif record.is_error_log and record.unique_id:
            self._node(record.unique_id).error_message = record.body
        elif (
            record.name == ARTIFACT_WRITTEN
            and record.record_type == SPAN_END
            and record.attributes.get("artifact_type") == ARTIFACT_TYPE_MANIFEST
        ):
            yield from self._on_end_of_run_manifest(record)
        elif record.name == INVOCATION and record.record_type == SPAN_END:
            self._invocation_end = record

    def _emits_events(self, record: OtelRecord) -> bool:
        return bool(record.unique_id and job_type_for_unique_id(record.unique_id))

    def _node(self, unique_id: str) -> _NodeState:
        if unique_id not in self._nodes:
            self._nodes[unique_id] = _NodeState(unique_id=unique_id)
        return self._nodes[unique_id]

    def _remember_node_identity(self, record: OtelRecord) -> None:
        if not record.unique_id:
            return
        node = self._node(record.unique_id)
        node.database = record.attributes.get("database")
        node.schema = record.attributes.get("schema")
        node.identifier = record.attributes.get("identifier")
        node.materialization = record.attributes.get("materialization")

    def _has_manifest_entry(self, unique_id: str) -> bool:
        return bool(self._node_metadata(unique_id))

    # ---------------------------------------------------------------- wrapper events

    def _wrapper_job_facets(self) -> dict:
        return {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType="JOB", integration="DBT", processingType="BATCH", producer=self.producer
            )
        }

    def _wrapper_run_facets(self) -> dict:
        dbt_run_facet = self.dbt_run_run_facet()
        if dbt_run := dbt_run_facet.get("dbt_run"):
            # What explains node events without inputs: no pre-parse manifest, and no manifest at the end.
            dbt_run.pre_parse = self._pre_parse
            dbt_run.write_json = self._write_json
        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **dbt_run_facet,
        }
        if self.parent_run_metadata:
            run_facets["parent"] = self.parent_run_metadata.to_openlineage()
        return run_facets

    def _wrapper_start_event(self, event_time: str, invocation_id: str | None) -> RunEvent:
        self._dbt_invocation_id = invocation_id
        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=event_time,
            run_id=str(generate_new_uuid()),
            job_name=self.job_name,
            job_namespace=self.job_namespace,
            job_facets=self._wrapper_job_facets(),
            run_facets=self._wrapper_run_facets(),
        )
        self._setup_dbt_run_metadata(start_event)
        self._wrapper_start = start_event
        return start_event

    def _wrapper_end_event(self) -> RunEvent:
        return_code = self.dbt_command_return_code
        run_facets = self._wrapper_run_facets()
        end = self._invocation_end
        if end is None and self._trace_id is not None and return_code != 0:
            # The invocation started but never ended, and dbt didn't exit cleanly: it was killed. With exit
            # code 0, dbt finished and only the Invocation end record went unread.
            event_type = RunState.ABORT
            event_time = nanos_to_iso(None)
        else:
            event_time = end.event_time() if end else nanos_to_iso(None)
            failed = return_code != 0 or (end is not None and end.status_code == STATUS_CODE_ERROR)
            event_type = RunState.FAIL if failed else RunState.COMPLETE
            if failed:
                message = (end.status_message if end else None) or f"dbt exited with code {return_code}"
                run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=message, programmingLanguage="sql"
                )
        assert self._wrapper_start is not None
        return generate_run_event(
            event_type=event_type,
            event_time=event_time,
            run_id=self._wrapper_start.run.runId,
            job_name=self._wrapper_start.job.name,
            job_namespace=self._wrapper_start.job.namespace,
            job_facets=self._wrapper_job_facets(),
            run_facets=run_facets,
        )

    # ---------------------------------------------------------------- node events

    def _node_run_facets(self, unique_id: str) -> dict:
        run_facets = {
            **self.dbt_version_facet(),
            **self.processing_engine_facet(),
            **self.dbt_run_run_facet(),
            "parent": self.dbt_run_metadata.to_openlineage(),
        }
        if self._has_manifest_entry(unique_id) and (
            tags_facet := self._build_tags_run_facet(
                self._get_node_tags(unique_id), self._get_node_meta(unique_id)
            )
        ):
            run_facets["tags"] = tags_facet
        return run_facets

    def _node_job_facets(self, node: _NodeState) -> dict:
        metadata = self._node_metadata(node.unique_id)
        test_type = None
        if node.unique_id.startswith("test."):
            test_type = "generic" if metadata.get("test_metadata") else "singular"
        job_facets: dict = {
            "jobType": job_type_job.JobTypeJobFacet(
                jobType=job_type_for_unique_id(node.unique_id),
                integration="DBT",
                processingType="BATCH",
                producer=self.producer,
            ),
            "dbt_node": DbtNodeJobFacet(
                original_file_path=metadata.get("original_file_path"),
                database=metadata.get("database", node.database),
                schema=metadata.get("schema", node.schema),
                alias=metadata.get("alias", node.identifier),
                unique_id=node.unique_id,
                test_type=test_type,
            ),
        }
        compiled_sql = self._compiled_sql(node)
        if compiled_sql and node.unique_id.startswith(("model.", "snapshot.")):
            job_facets["sql"] = sql_job.SQLJobFacet(query=compiled_sql, dialect=self.extract_dialect())
        return job_facets

    def _compiled_sql(self, node: _NodeState) -> str | None:
        metadata = self._node_metadata(node.unique_id)
        return node.compiled_sql or metadata.get("compiled_code") or metadata.get("compiled_sql")

    def _is_ephemeral(self, node: _NodeState) -> bool:
        if node.materialization == NODE_MATERIALIZATION_EPHEMERAL:
            return True
        return (self._node_metadata(node.unique_id).get("config") or {}).get("materialized") == "ephemeral"

    def _output_node(self, node: _NodeState) -> ModelNode | None:
        """The relation the node writes: from the manifest when it has the node, else from telemetry.

        An ephemeral model is inlined into the models that use it and writes no relation.
        """
        if self._is_ephemeral(node):
            return None
        if self._has_manifest_entry(node.unique_id):
            return self._get_model_node(node.unique_id)
        if not node.identifier:
            return None
        return ModelNode(
            type="model",
            metadata_node={
                "database": node.database,
                "schema": node.schema,
                "alias": node.identifier,
                "name": node.identifier,
                "unique_id": node.unique_id,
                "columns": {},
            },
        )

    def _node_inputs(self, unique_id: str) -> list[ModelNode]:
        if not self._has_manifest_entry(unique_id):
            return []
        return self._get_model_inputs(unique_id)

    def _node_start_event(self, record: OtelRecord) -> RunEvent:
        unique_id = record.unique_id
        assert unique_id is not None
        node = self._node(unique_id)
        node.started = True
        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []
        # A unit test runs against mocked rows in a temporary relation: it reads and writes no dataset.
        if not unique_id.startswith("unit_test."):
            inputs = [self.node_to_dataset(node=n, has_facets=True) for n in self._node_inputs(unique_id)]  # type: ignore[misc]
            if output_node := self._output_node(node):
                outputs = [self.node_to_output_dataset(node=output_node, has_facets=True)]
        run_facets = self._node_run_facets(unique_id)
        if extraction_error := self._get_extraction_error_facet(unique_id):
            run_facets["extractionError"] = extraction_error
        return generate_run_event(
            event_type=RunState.START,
            event_time=record.event_time(),
            run_id=self.node_id_to_ol_run_id[unique_id],
            job_name=unique_id,
            job_namespace=self.job_namespace,
            job_facets=self._node_job_facets(node),
            run_facets=run_facets,
            inputs=inputs,
            outputs=outputs,
        )

    def _on_node_end(self, record: OtelRecord) -> Iterator[RunEvent]:
        unique_id = record.unique_id
        assert unique_id is not None
        if not self._has_manifest_entry(unique_id) and not unique_id.startswith("unit_test."):
            # Inputs need a manifest entry; wait for the manifest dbt writes after the RUN phase.
            self._held_node_ends.append(record)
            return
        yield self._node_end_event(record)

    def _on_end_of_run_manifest(self, record: OtelRecord) -> Iterator[RunEvent]:
        if not self._held_node_ends:
            return
        self._load_end_of_run_manifest(record)
        yield from self._release_held_node_ends()

    def _load_end_of_run_manifest(self, record: OtelRecord) -> None:
        relative_path = record.attributes.get("relative_path")
        candidates = [self.manifest_path]
        if relative_path:
            candidates += [os.path.join(self.project_dir, relative_path), os.path.abspath(relative_path)]
        for path in candidates:
            if not os.path.isfile(path):
                continue
            if self._dbt_started_at is not None and os.path.getmtime(path) < self._dbt_started_at - 2.0:
                continue
            try:
                self._compiled_manifest = self.load_metadata(path, list(range(2, 13)), self.logger)
                self.node_id_to_inputs.clear()
                return
            except (OSError, ValueError):
                self.logger.warning("Could not load the end-of-run manifest %s", path, exc_info=True)

    def _release_held_node_ends(self) -> Iterator[RunEvent]:
        held, self._held_node_ends = self._held_node_ends, []
        for node_end in held:
            # Each held end is a record of its own: one that fails mustn't drop the ones after it.
            try:
                event = self._node_end_event(node_end)
            except Exception:
                self._skip_failed_record()
                continue
            yield event

    def _node_end_event(self, record: OtelRecord) -> RunEvent:
        unique_id = record.unique_id
        assert unique_id is not None
        node = self._node(unique_id)
        attributes = record.attributes
        outcome = attributes.get("node_outcome")
        test_detail = attributes.get("node_test_detail") or {}
        test_outcome = test_detail.get("test_outcome")
        failing_rows = test_detail.get("failing_rows")
        failing_rows = int(failing_rows) if failing_rows is not None else None
        if failing_rows is None and outcome == NODE_OUTCOME_SUCCESS and unique_id.startswith("test."):
            # A passing test reports an empty node_test_detail: no failing rows.
            failing_rows = 0

        run_facets = self._node_run_facets(unique_id)
        error_message: str | None = None
        if outcome == NODE_OUTCOME_SUCCESS:
            if test_outcome == TEST_OUTCOME_FAILED:
                event_type = RunState.FAIL
                error_message = self._test_failure_message(unique_id, failing_rows, test_detail)
            else:
                event_type = RunState.COMPLETE
        elif outcome == NODE_OUTCOME_ERROR:
            event_type = RunState.FAIL
            error_message = node.error_message or node.failed_query_message or record.status_message
        elif outcome == NODE_OUTCOME_SKIPPED:
            # Ephemeral models end as a no-op skip; dbt Core v1 reported them as successful.
            no_op = attributes.get("node_skip_reason") == NODE_SKIP_REASON_NO_OP
            event_type = RunState.COMPLETE if no_op else RunState.ABORT
        elif outcome == NODE_OUTCOME_CANCELED:
            event_type = RunState.ABORT
        else:
            self.logger.info("Node %s has an unknown outcome %s", unique_id, outcome)
            event_type = RunState.OTHER
        if error_message:
            run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                message=error_message, programmingLanguage="sql"
            )

        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []
        if not unique_id.startswith("unit_test."):
            inputs, outputs = self._node_end_datasets(
                unique_id, self._output_node(node), event_type, self._compiled_sql(node)
            )
        # Assertions only for tests that ran; a passing test has an empty node_test_detail.
        if unique_id.startswith("test.") and outcome in (NODE_OUTCOME_SUCCESS, NODE_OUTCOME_ERROR):
            success = outcome == NODE_OUTCOME_SUCCESS and test_outcome not in (
                TEST_OUTCOME_FAILED,
                TEST_OUTCOME_WARNED,
            )
            if self._has_manifest_entry(unique_id):
                inputs = self._test_datasets_with_assertion(unique_id, success, failing_rows) or inputs
                if test_facet := self._singular_test_facet(unique_id, success, failing_rows):
                    run_facets["test"] = test_facet

        if extraction_error := self._get_extraction_error_facet(unique_id):
            run_facets["extractionError"] = extraction_error
        self._nodes.pop(unique_id, None)
        return generate_run_event(
            event_type=event_type,
            event_time=record.event_time(),
            run_id=self.node_id_to_ol_run_id[unique_id],
            job_name=unique_id,
            job_namespace=self.job_namespace,
            job_facets=self._node_job_facets(node),
            run_facets=run_facets,
            inputs=inputs,
            outputs=outputs,
        )

    def _test_failure_message(self, unique_id: str, failing_rows: int | None, test_detail: dict) -> str:
        """The message dbt Core v1 gives a failed test, which dbt Core v2 telemetry doesn't carry."""
        if unique_id.startswith("unit_test."):
            return test_detail.get("diff_table") or "Unit test failed"
        config = self._node_metadata(unique_id).get("config") or {}
        threshold = config.get("error_if") or "!= 0"
        return f"Got {_pluralize(failing_rows or 0, 'result')}, configured to fail if {threshold}"

    # ---------------------------------------------------------------- query events

    def _query_start_event(self, record: OtelRecord) -> RunEvent:
        unique_id = record.unique_id
        assert unique_id is not None and record.span_id is not None
        node = self._node(unique_id)
        node.query_count += 1
        parent_run = ParentRunMetadata(
            run_id=self.node_id_to_ol_run_id[unique_id],
            job_name=unique_id,
            job_namespace=self.job_namespace,
            root_parent_run_id=self.get_root_parent_run_id(),
            root_parent_job_namespace=self.get_root_parent_job_namespace(),
            root_parent_job_name=self.get_root_parent_job_name(),
        )
        start_event = generate_run_event(
            event_type=RunState.START,
            event_time=record.event_time(),
            run_id=str(generate_new_uuid()),
            job_name=f"{unique_id}.sql.{node.query_count}",
            job_namespace=self.job_namespace,
            job_facets={
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType="SQL", integration="DBT", processingType="BATCH", producer=self.producer
                ),
                "sql": sql_job.SQLJobFacet(
                    query=record.attributes.get("sql") or "", dialect=self.extract_dialect()
                ),
            },
            run_facets={
                **self.dbt_version_facet(),
                **self.processing_engine_facet(),
                **self.dbt_run_run_facet(),
                "parent": parent_run.to_openlineage(),
            },
        )
        self._queries[record.span_id] = _QueryState(unique_id=unique_id, start_event=start_event)
        return start_event

    def _query_end_event(self, record: OtelRecord) -> RunEvent:
        assert record.span_id is not None
        query = self._queries.pop(record.span_id)
        run_facets = dict(query.start_event.run.facets or {})
        if record.attributes.get("query_outcome") == QUERY_OUTCOME_SUCCESS:
            event_type = RunState.COMPLETE
            if query_id := record.attributes.get("query_id"):
                run_facets["externalQuery"] = external_query_run.ExternalQueryRunFacet(
                    externalQueryId=query_id, source=self.dataset_namespace
                )
        else:
            event_type = RunState.FAIL
            message = record.attributes.get("query_error_adapter_message") or record.status_message
            if message:
                run_facets["errorMessage"] = error_message_run.ErrorMessageRunFacet(
                    message=message, programmingLanguage="sql"
                )
                self._node(query.unique_id).failed_query_message = message
        return generate_run_event(
            event_type=event_type,
            event_time=record.event_time(),
            run_id=query.start_event.run.runId,
            job_name=query.start_event.job.name,
            job_namespace=query.start_event.job.namespace,
            job_facets=query.start_event.job.facets,
            run_facets=run_facets,
        )

    # ---------------------------------------------------------------- end of run

    def _finalize(self) -> Iterator[RunEvent]:
        """Close everything once dbt has exited.

        A killed dbt writes no span ends, so runs still open are aborted here.
        """
        if self._held_node_ends:
            # No ArtifactWritten record for the manifest: dbt was killed, crashed or ran with --no-write-json.
            yield from self._release_held_node_ends()
        aborted_at = nanos_to_iso(None)
        for query in list(self._queries.values()):
            yield generate_run_event(
                event_type=RunState.ABORT,
                event_time=aborted_at,
                run_id=query.start_event.run.runId,
                job_name=query.start_event.job.name,
                job_namespace=query.start_event.job.namespace,
                job_facets=query.start_event.job.facets,
                run_facets=query.start_event.run.facets,
            )
        self._queries.clear()
        for node in [n for n in self._nodes.values() if n.started]:
            yield generate_run_event(
                event_type=RunState.ABORT,
                event_time=aborted_at,
                run_id=self.node_id_to_ol_run_id[node.unique_id],
                job_name=node.unique_id,
                job_namespace=self.job_namespace,
                job_facets=self._node_job_facets(node),
                run_facets=self._node_run_facets(node.unique_id),
            )
        self._nodes.clear()

        if self._wrapper_start is None:
            # dbt exited before writing any telemetry, for example on invalid arguments.
            if self.dbt_command_return_code == 0:
                self.logger.warning(
                    "Found no telemetry for this dbt run in %s; sent no OpenLineage events",
                    self.otel_file_path,
                )
                return
            yield self._wrapper_start_event(nanos_to_iso(None), invocation_id=None)
        yield self._wrapper_end_event()
