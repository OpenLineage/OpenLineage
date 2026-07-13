# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import collections
import datetime
import json
import logging
from abc import abstractmethod
from collections.abc import Sequence
from enum import Enum
from functools import cached_property
from typing import Any

import attr
from openlineage.client.event_v2 import Dataset, InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    DatasetFacet,
    InputDatasetFacet,
    JobFacet,
    OutputDatasetFacet,
    RunFacet,
    column_lineage_dataset,
    data_quality_assertions_dataset,
    datasource_dataset,
    documentation_dataset,
    external_query_run,
    job_type_job,
    output_statistics_output_dataset,
    ownership_dataset,
    processing_engine_run,
    schema_dataset,
    sql_job,
    tags_run,
    test_run,
)
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import (
    DbtExposure,
    DbtExposuresDatasetFacet,
    DbtIncrementalConfig,
    DbtModelConfig,
    DbtModelDatasetFacet,
    DbtNodeJobFacet,
    DbtPartitionBy,
    DbtRunRunFacet,
    DbtVersionRunFacet,
    ParentRunMetadata,
)
from openlineage.common.provider.dbt.utils import __version__ as openlineage_version
from openlineage.common.provider.snowflake import fix_account_name
from openlineage.common.utils import get_from_multiple_chains, get_from_nullable_chain
from openlineage_sql import parse as parse_sql


class Adapter(Enum):
    # This class represents supported adapters.
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"
    POSTGRES = "postgres"
    DATABRICKS = "databricks"
    SQLSERVER = "sqlserver"
    DREMIO = "dremio"
    ATHENA = "athena"
    DUCKDB = "duckdb"
    TRINO = "trino"
    GLUE = "glue"
    CLICKHOUSE = "clickhouse"
    FABRIC = "fabric"

    @staticmethod
    def adapters() -> str:
        # String representation of all supported adapter names
        return ",".join([f"`{x.value}`" for x in list(Adapter)])


class SparkConnectionMethod(Enum):
    THRIFT = "thrift"
    ODBC = "odbc"
    HTTP = "http"

    @staticmethod
    def methods():
        return [x.value for x in SparkConnectionMethod]


class UnsupportedDbtCommand(Exception):
    pass


def expected_from_test_config(config: dict | None, severity: str | None) -> str:
    """Translate dbt's ``error_if``/``warn_if`` threshold into the spec's ``expected`` value.

    dbt's default is ``"!= 0"`` (any failing row trips the test) — semantically the
    expected count is 0. A non-default threshold like ``"> 10"`` is carried through
    so a consumer can distinguish "expected zero failures, got N" from "expected at
    most 10 failures, got N".
    """
    threshold_key = "warn_if" if (severity or "").lower() == "warn" else "error_if"
    threshold = (config or {}).get(threshold_key)
    if threshold is None or "".join(str(threshold).split()) == "!=0":
        return "0"
    return str(threshold)


@attr.define
class ModelNode:
    # in reality Literal["model", "test", "snapshot", "source", "seed"]
    # but way too much work to convince mypy that it's true
    type: str
    metadata_node: dict
    catalog_node: dict | None = None


@attr.define
class DbtRun:
    started_at: str
    completed_at: str
    status: str
    inputs: list[Dataset]
    output: Dataset | None
    job_name: str
    namespace: str
    run_id: str = attr.field(factory=lambda: str(generate_new_uuid()))


@attr.define
class DbtRunResult:
    start: RunEvent
    complete: RunEvent | None = None
    fail: RunEvent | None = None


@attr.define
class DbtEvents:
    starts: list[RunEvent] = attr.field(factory=list)
    completes: list[RunEvent] = attr.field(factory=list)
    fails: list[RunEvent] = attr.field(factory=list)

    def events(self):
        return self.starts + self.completes + self.fails

    def add(self, item: DbtRunResult | None):
        if not item:
            return
        self.starts.append(item.start)
        if item.complete:
            self.completes.append(item.complete)
        if item.fail:
            self.fails.append(item.fail)

    def __iadd__(self, other):
        if isinstance(other, DbtEvents):
            self.starts += other.starts
            self.completes += other.completes
            self.fails += other.fails
            return self
        raise NotImplementedError


@attr.define
class DbtRunContext:
    manifest: dict
    run_results: dict
    catalog: dict | None = None


class DbtArtifactProcessor:
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        producer: str,
        job_namespace: str,
        skip_errors: bool = False,
        logger: logging.Logger | None = None,
        models: Sequence[str] | None = None,
        selector: str | None = None,
        openlineage_job_name: str | None = None,
    ):
        self.producer = producer
        self._dbt_run_metadata: ParentRunMetadata | None = None
        self.logger = logger or logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

        self.openlineage_job_name = openlineage_job_name
        self.job_namespace = job_namespace
        self.dataset_namespace = ""
        self.skip_errors = skip_errors
        self.run_metadata: dict[str, Any] = {}
        self.command = None
        self.full_refresh: bool | None = None
        self.models = models or []
        self.selector = selector
        self.manifest_version = None
        self.adapter_type: Adapter | None = None
        # Populated by parse(); retained so manifest-declared exposures can be indexed
        # and attached to the datasets they depend on.
        self.manifest: dict[str, Any] = {}

    @property
    def dbt_run_metadata(self):
        return self._dbt_run_metadata

    @dbt_run_metadata.setter
    def dbt_run_metadata(self, metadata: ParentRunMetadata):
        self._dbt_run_metadata = metadata

    @abstractmethod
    def get_dbt_metadata(self): ...

    def parse(self) -> DbtEvents:
        """
        Parse dbt manifest and run_result and produce OpenLineage events.
        """
        manifest, run_result, profile, catalog = self.get_dbt_metadata()
        self.manifest = manifest
        self.manifest_version = self.get_schema_version(manifest)

        self.run_metadata = run_result["metadata"]
        self.command = run_result["args"]["which"]
        self.full_refresh = self._full_refresh_from_args(run_result["args"])

        self.extract_adapter_type(profile)
        self.extract_dataset_namespace(profile)

        nodes = {}
        # Filter nodes
        for name, node in manifest["nodes"].items():
            if any(name.startswith(prefix) for prefix in ("model.", "test.", "snapshot.")):
                nodes[name] = node

        context = DbtRunContext(manifest, run_result, catalog)

        events = DbtEvents()

        if self.command not in ["run", "build", "test", "seed", "snapshot"]:
            if self.should_raise_on_unsupported_command:
                raise UnsupportedDbtCommand(
                    f"Not recognized run command {self.command} - "
                    "should be run, build, test, seed or snapshot"
                )
            else:
                return events

        if self.command in ["run", "build", "seed", "snapshot"]:
            events += self.parse_execution(context, nodes)
        if self.command in ["test", "build"]:
            events += self.parse_test(context, nodes)
        return events

    @classmethod
    def get_schema_version(cls, metadata):
        str_schema_version = get_from_nullable_chain(metadata, ["metadata", "dbt_schema_version"])
        return cls.get_version_number(str_schema_version)

    def get_query_id(self, run_result: dict[str, Any]) -> str | None:
        # Validate that there is an adapter_response in the run_result
        if "adapter_response" not in run_result:
            return None

        # Default to query_id for all Adapters
        query_id_key: str = "query_id"
        query_id: str | None

        # Use the adapter type to make sure the correct key is used
        if self.adapter_type == Adapter.BIGQUERY:
            query_id_key = "job_id"
            project_id: str | None = run_result["adapter_response"].get("project_id")
            location: str | None = run_result["adapter_response"].get("location")
            job_id: str | None = run_result["adapter_response"].get(query_id_key)
            if project_id is not None and location is not None and job_id is not None:
                query_id = f"{project_id}:{location}.{job_id}"
            else:
                query_id = job_id
        else:
            query_id = run_result["adapter_response"].get(query_id_key)

        if isinstance(query_id, str):
            # For Databricks, "N/A" could be returned if the query_id is None; catch that
            return None if query_id.lower() == "n/a" else query_id

        return query_id

    @staticmethod
    def get_version_number(version: str) -> int:
        # "https://schemas.getdbt.com/dbt/manifest/v2.json" -> "v2.json"
        file = version.split("/")[-1]
        # "v2.json" -> 2
        return int(file.split(".")[0][1:])

    def parse_execution(self, context: DbtRunContext, nodes: dict) -> DbtEvents:
        events = DbtEvents()
        for run in context.run_results["results"]:
            name = run["unique_id"]

            # Pull the available query_id from the run_results
            query_id: str | None = self.get_query_id(run)

            if not any(name.startswith(prefix) for prefix in ("model.", "source.", "snapshot.")):
                continue
            if run["status"] == "skipped":
                continue

            output_node = nodes[name]
            started_at, completed_at = self.get_timings(run["timing"])

            # (node_id, node) so exposures depending on an input can be looked up below.
            inputs: list[tuple[str, ModelNode]] = []
            for node in context.manifest["parent_map"][run["unique_id"]]:
                if node.startswith("model."):
                    inputs.append(
                        (
                            node,
                            ModelNode(
                                type="model",
                                metadata_node=nodes[node],
                                catalog_node=get_from_nullable_chain(context.catalog, ["nodes", node]),
                            ),
                        )
                    )
                elif node.startswith("source."):
                    inputs.append(
                        (
                            node,
                            ModelNode(
                                type="source",
                                metadata_node=context.manifest["sources"][node],
                                catalog_node=get_from_nullable_chain(context.catalog, ["sources", node]),
                            ),
                        )
                    )

            run_id = str(generate_new_uuid())
            if name.startswith("snapshot."):
                jobType = "SNAPSHOT"
                job_name = self._format_dataset_name(
                    output_node["database"],
                    output_node["schema"],
                    self.removeprefix(run["unique_id"], "snapshot."),
                ) + (".build.snapshot" if self.command == "build" else ".snapshot")
            else:
                jobType = "MODEL"
                job_name = self._format_dataset_name(
                    output_node["database"],
                    output_node["schema"],
                    self.removeprefix(run["unique_id"], "model."),
                ) + (".build.run" if self.command == "build" else "")

            if self.manifest_version >= 7:  # type: ignore
                sql = output_node.get("compiled_code", None)
            else:
                sql = output_node["compiled_sql"]

            job_facets: dict[str, JobFacet] = {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType=jobType,
                    integration="DBT",
                    processingType="BATCH",
                    producer=self.producer,
                ),
                "dbt_node": DbtNodeJobFacet(
                    original_file_path=output_node.get("original_file_path"),
                    database=output_node.get("database"),
                    schema=output_node.get("schema"),
                    alias=output_node.get("alias"),
                    unique_id=output_node.get("unique_id"),
                ),
            }
            if sql:
                job_facets["sql"] = sql_job.SQLJobFacet(query=sql, dialect=self.extract_dialect())

            run_facets: dict[str, RunFacet] = {}
            if tags_facet := self._build_tags_run_facet(output_node.get("tags"), output_node.get("meta")):
                run_facets["tags"] = tags_facet

            output_dataset = self.node_to_output_dataset(
                ModelNode(
                    type=jobType.lower(),
                    metadata_node=output_node,
                    catalog_node=get_from_nullable_chain(context.catalog, ["nodes", run["unique_id"]]),
                ),
                has_facets=True,
                adapter_response=run.get("adapter_response", None),
            )

            # Add column lineage if SQL is available
            if sql:
                column_lineage = self.get_column_lineage(output_dataset.namespace, sql)
                if column_lineage:
                    output_dataset.facets["columnLineage"] = column_lineage  # type: ignore

            # Attach exposures only for successful builds (FAIL emits no outputs, skipped
            # nodes are filtered above).
            succeeded = run["status"] == "success"
            input_datasets = []
            for node_id, input_node in inputs:
                input_dataset = self.node_to_dataset(input_node, has_facets=True)
                if succeeded:
                    input_dataset.facets.update(self._exposures_facets(node_id))  # type: ignore
                input_datasets.append(input_dataset)

            if succeeded:
                output_dataset.facets.update(self._exposures_facets(run["unique_id"]))  # type: ignore

            events.add(
                self.to_openlineage_events(
                    run["status"],
                    started_at,
                    completed_at,
                    self.get_run(run_id=run_id, query_id=query_id, run_facets=run_facets),
                    Job(namespace=self.job_namespace, name=job_name, facets=job_facets),
                    input_datasets,
                    output_dataset,
                )
            )
        return events

    def parse_test(self, context: DbtRunContext, nodes: dict) -> DbtEvents:
        # The tests can have different timings, so just take current time
        started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        completed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        assertions = self.parse_assertions(context, nodes)

        events = DbtEvents()
        manifest_nodes = {**context.manifest["nodes"], **context.manifest["sources"]}
        for unique_id, node in manifest_nodes.items():
            if unique_id.startswith("model."):
                node_type = "model"
            elif unique_id.startswith("source."):
                node_type = "source"
            else:
                continue
            node_assertions = assertions[unique_id]
            if len(node_assertions) == 0:
                continue

            assertion_facet = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=node_assertions
            )

            namespace, name, _, _ = self.extract_dataset_data(
                ModelNode(type=node_type, metadata_node=node), assertion_facet, has_facets=False
            )

            job_name = self._format_dataset_name(
                node["database"],
                node["schema"],
                self.removeprefix(node["unique_id"], "model."),
            ) + (".build.test" if self.command == "build" else ".test")

            job_facets: dict[str, JobFacet] = {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType="TEST",
                    integration="DBT",
                    processingType="BATCH",
                    producer=self.producer,
                ),
                "dbt_node": DbtNodeJobFacet(
                    original_file_path=node.get("original_file_path"),
                    database=node.get("database"),
                    schema=node.get("schema"),
                    alias=node.get("alias"),
                    unique_id=node.get("unique_id"),
                ),
            }

            run_facets: dict[str, RunFacet] = {}
            if tags_facet := self._build_tags_run_facet(node.get("tags"), node.get("meta")):
                run_facets["tags"] = tags_facet

            run_id = str(generate_new_uuid())
            dataset_facets: dict[str, InputDatasetFacet] = {"dataQualityAssertions": assertion_facet}
            # The aggregate per-model test event is FAIL when any of its assertions failed.
            # Warn-severity failures count as success so they don't block the pipeline,
            # mirroring dbt's own success/failure semantics.
            status = (
                "success"
                if all(a.success or (a.severity or "").lower() == "warn" for a in node_assertions)
                else "error"
            )
            events.add(
                self.to_openlineage_events(
                    status,
                    started_at,
                    completed_at,
                    self.get_run(run_id=run_id, run_facets=run_facets),
                    Job(namespace=self.job_namespace, name=job_name, facets=job_facets),
                    [
                        InputDataset(
                            namespace=namespace,
                            name=name,
                            inputFacets=dataset_facets,
                            # TODO: remove this next release
                            facets=dataset_facets,  # type: ignore
                        )
                    ],
                    None,
                )
            )

        # Emit per-test events with TestRunFacet
        parent_map = context.manifest.get("parent_map", {})
        for run in context.run_results["results"]:
            if not run["unique_id"].startswith("test."):
                continue
            test_node = nodes.get(run["unique_id"])
            if not test_node:
                continue

            # Singular tests have no test_metadata (they are plain SQL files).
            # Generic tests (not_null, unique, custom generic) have test_metadata with a name.
            # Only singular tests get a per-test run event with TestRunFacet; generic tests
            # are covered by the DataQualityAssertions dataset facet emitted in the loop above.
            if test_node.get("test_metadata") is not None:
                continue

            inputs = self._resolve_test_inputs(run["unique_id"], parent_map, manifest_nodes)
            test_obj = self._build_test_execution(run, test_node, inputs)

            # A test run "succeeds" (does not block the pipeline) when status is
            # "pass" or "warn". The warn/error severity distinction is captured in
            # TestExecution.severity — "warn" tests don't raise the run-level error.
            run_status = "success" if run["status"] in {"pass", "warn"} else "error"

            run_facets_per_test: dict[str, RunFacet] = {
                "test": test_run.TestRunFacet(tests=[test_obj]),
            }
            if tags_facet := self._build_tags_run_facet(test_node.get("tags"), test_node.get("meta")):
                run_facets_per_test["tags"] = tags_facet

            job_facets_per_test: dict[str, JobFacet] = {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType="TEST",
                    integration="DBT",
                    processingType="BATCH",
                    producer=self.producer,
                ),
                "dbt_node": DbtNodeJobFacet(
                    original_file_path=test_node.get("original_file_path"),
                    database=test_node.get("database"),
                    schema=test_node.get("schema"),
                    alias=test_node.get("alias"),
                    unique_id=test_node.get("unique_id"),
                ),
            }

            events.add(
                self.to_openlineage_events(
                    run_status,
                    started_at,
                    completed_at,
                    self.get_run(run_id=str(generate_new_uuid()), run_facets=run_facets_per_test),
                    Job(namespace=self.job_namespace, name=run["unique_id"], facets=job_facets_per_test),
                    inputs,
                    None,
                )
            )

        return events

    def _resolve_test_inputs(
        self, unique_id: str, parent_map: dict, manifest_nodes: dict
    ) -> list[InputDataset]:
        """Return InputDatasets for the model/source/seed parents of a test node."""
        inputs: list[InputDataset] = []
        for parent_id in parent_map.get(unique_id, []):
            if not any(parent_id.startswith(p) for p in ["model.", "source.", "seed."]):
                continue
            parent_node = manifest_nodes.get(parent_id)
            if parent_node:
                ptype = "source" if parent_id.startswith("source.") else "model"
                ns, nm, _, _ = self.extract_dataset_data(
                    ModelNode(type=ptype, metadata_node=parent_node), None, has_facets=False
                )
                inputs.append(InputDataset(namespace=ns, name=nm))
        return inputs

    def _build_test_execution(
        self, run: dict, test_node: dict, inputs: list[InputDataset]
    ) -> test_run.TestExecution:
        """Build a TestExecution from a run result and its manifest node.

        When no input datasets could be resolved (e.g. singular tests), enriches
        the object with type, SQL content, params, and description.
        """
        config = test_node.get("config", {})
        severity = (config.get("severity") or "error").lower()
        test_metadata = test_node.get("test_metadata")

        obj = test_run.TestExecution(
            name=run["unique_id"],
            status="pass" if run["status"] == "pass" else "fail",
            severity=severity,
        )

        # dbt reports the count of violating rows as `failures` in run_results.json.
        # `expected` mirrors the test's error_if/warn_if threshold (default "!= 0" → "0").
        failures = run.get("failures")
        if failures is not None:
            obj.actual = str(failures)
            obj.expected = expected_from_test_config(config, severity)

        if not inputs:
            obj.type = test_metadata["name"] if test_metadata else "singular"
            obj.content = (
                run.get("compiled_code") or test_node.get("compiled_code") or test_node.get("raw_code")
            )
            obj.contentType = "sql"
            if test_metadata:
                params = {k: v for k, v in test_metadata.get("kwargs", {}).items() if k != "model"}
                if params:
                    obj.params = params
            if desc := test_node.get("description"):
                obj.description = desc

        return obj

    def parse_assertions(
        self, context: DbtRunContext, nodes: dict
    ) -> dict[str, list[data_quality_assertions_dataset.Assertion]]:
        assertions = collections.defaultdict(list)
        for run in context.run_results["results"]:
            if not run["unique_id"].startswith("test."):
                continue
            test_node = nodes[run["unique_id"]]

            model_node = None
            for node in context.manifest["parent_map"][run["unique_id"]]:
                if any(node.startswith(prefix) for prefix in ["model.", "source.", "seed."]):
                    model_node = node

            if self.manifest_version >= 12:  # type: ignore
                name = test_node["name"]
                node_columns = test_node

            else:
                test_metadata = test_node.get("test_metadata")
                if test_metadata:
                    name = test_metadata["name"]
                    node_columns = test_metadata
                else:
                    # Singular test — no test_metadata, use node name directly
                    name = test_node["name"]
                    node_columns = test_node

            # Extract severity from config, normalize to lowercase
            config = test_node.get("config", {})
            severity = config.get("severity")
            if severity:
                severity = severity.lower()

            failures = run.get("failures")
            actual = str(failures) if failures is not None else None
            expected = expected_from_test_config(config, severity) if failures is not None else None

            assertions[model_node].append(
                data_quality_assertions_dataset.Assertion(
                    assertion=name,
                    success=True if run["status"] == "pass" else False,
                    column=get_from_nullable_chain(node_columns, ["kwargs", "column_name"]),
                    severity=severity,
                    actual=actual,
                    expected=expected,
                )
            )

            if not model_node:
                self.logger.warning(f"Model node connected to test {nodes[run['unique_id']]} not found")
        return assertions  # type: ignore

    def to_openlineage_events(self, *args, **kwargs) -> DbtRunResult | None:
        try:
            return self._to_openlineage_events(*args, **kwargs)
        except Exception as e:
            if self.skip_errors:
                return None
            raise ValueError from e

    def _to_openlineage_events(
        self,
        status: str,
        started_at: str,
        completed_at: str,
        run: Run,
        job: Job,
        inputs: list[InputDataset],
        output: OutputDataset | None,
    ) -> DbtRunResult | None:
        start = RunEvent(
            eventType=RunState.START,
            eventTime=started_at,
            run=run,
            job=job,
            producer=self.producer,
            inputs=inputs,
            outputs=[output] if output else [],
        )
        if status == "success":
            return DbtRunResult(
                start,
                complete=RunEvent(
                    eventType=RunState.COMPLETE,
                    eventTime=completed_at,
                    run=run,
                    job=job,
                    producer=self.producer,
                    inputs=inputs,
                    outputs=[output] if output else [],
                ),
            )
        elif status == "error":
            return DbtRunResult(
                start,
                fail=RunEvent(
                    eventType=RunState.FAIL,
                    eventTime=completed_at,
                    run=run,
                    job=job,
                    producer=self.producer,
                    inputs=inputs,
                    outputs=[],
                ),
            )
        else:
            # Should not happen?
            raise ValueError(f"Run status was {status}, should be in ['success', 'skipped', 'error']")

    def node_to_dataset(
        self,
        node: ModelNode,
        has_facets: bool = False,
    ) -> Dataset:
        namespace, name, facets, _ = self.extract_dataset_data(node, None, has_facets)
        return Dataset(name=name, namespace=namespace, facets=facets)

    def node_to_output_dataset(
        self,
        node: ModelNode,
        has_facets: bool = False,
        adapter_response: dict | None = None,
    ) -> OutputDataset:
        namespace, name, facets, _ = self.extract_dataset_data(node, None, has_facets)
        if not has_facets:
            return OutputDataset(name=name, namespace=namespace, facets=facets)

        row_count = 0
        byte_count = 0
        if node.catalog_node:
            row_count = (
                get_from_multiple_chains(
                    node.catalog_node,
                    [
                        ["stats", "num_rows", "value"],  # bigquery
                        ["stats", "row_count", "value"],  # snowflake
                        ["stats", "rows", "value"],  # redshift
                    ],
                )
                or 0
            )

            byte_count = (
                get_from_multiple_chains(
                    node.catalog_node,
                    [
                        ["stats", "num_bytes", "value"],  # bigquery
                        ["stats", "bytes", "value"],  # snowflake
                    ],
                )
                or 0
            )

            if self.adapter_type == Adapter.REDSHIFT:
                # size is the number of 1MB blocks
                blocks_count = (
                    get_from_multiple_chains(
                        node.catalog_node,
                        [
                            ["stats", "size", "value"],
                        ],
                    )
                    or 0
                )
                byte_count = byte_count or blocks_count * (2**20)

        if adapter_response:
            row_count = row_count or adapter_response.get("rows_affected", 0)
            byte_count = byte_count or adapter_response.get("bytes_processed", 0)

        row_count = int(row_count)
        byte_count = int(byte_count)
        if not row_count and not byte_count:
            return OutputDataset(name=name, namespace=namespace, facets=facets)

        output_facets: dict[str, OutputDatasetFacet] = {}
        output_facets["outputStatistics"] = (
            output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
                rowCount=row_count if row_count > 0 else None,
                size=byte_count if byte_count > 0 else None,
            )
        )
        return OutputDataset(name=name, namespace=namespace, facets=facets, outputFacets=output_facets)

    def _format_dataset_name(self, database: str | None, schema: str | None, table: str) -> str:
        return ".".join(list(filter(None, [database, schema, table])))

    def extract_dataset_data(
        self,
        node: ModelNode,
        assertions: data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet | None,
        has_facets: bool = False,
    ) -> tuple[str, str, dict, dict]:
        facets: dict[str, DatasetFacet]
        input_facets: dict[str, InputDatasetFacet] = {}
        if has_facets:
            facets = {
                "dataSource": datasource_dataset.DatasourceDatasetFacet(
                    name=self.dataset_namespace, uri=self.dataset_namespace
                ),
            }

            documentation = node.metadata_node.get("description", "")
            if documentation:
                facets["documentation"] = documentation_dataset.DocumentationDatasetFacet(
                    description=documentation
                )

            if assertions:
                input_facets["dataQualityAssertions"] = assertions

            if node.catalog_node:
                fields = self.extract_catalog_fields(
                    node.catalog_node["columns"].values(),
                    node.metadata_node["columns"],
                )
            else:
                fields = self.extract_metadata_fields(node.metadata_node["columns"].values())
            if fields:
                facets["schema"] = schema_dataset.SchemaDatasetFacet(fields=fields)

            if owner := get_from_nullable_chain(node.metadata_node, ["meta", "owner"]):
                names = owner if isinstance(owner, list) else [owner]
                facets["ownership"] = ownership_dataset.OwnershipDatasetFacet(
                    owners=[ownership_dataset.Owner(name=name) for name in names]
                )

            if dbt_model_facet := self._create_dbt_model_dataset_facet(node):
                facets["dbt_model"] = dbt_model_facet
        else:
            facets = {}
        if node.type == "source":
            table = node.metadata_node["name"]
        else:
            table = node.metadata_node["alias"]
        return (
            self.dataset_namespace,
            self._format_dataset_name(
                node.metadata_node["database"],
                node.metadata_node["schema"],
                table,
            ),
            facets,
            input_facets,
        )

    def _create_dbt_model_dataset_facet(self, node: ModelNode) -> DbtModelDatasetFacet | None:
        """Build a DbtModelDatasetFacet from a dbt manifest node's resolved ``config``.

        Reads ``materialized``/``access``/``owner``/``group`` from ``node.metadata_node["config"]``,
        plus the incremental rebuild config for ``materialized == "incremental"`` models.
        Returns ``None`` when the node carries none of them, so we don't attach an empty facet.
        The node's ``meta`` map is emitted separately as tags (see ``_build_tags_run_facet``).
        """
        raw_config = node.metadata_node.get("config") or {}
        model_config = DbtModelConfig(
            materialized=raw_config.get("materialized") or None,
            access=raw_config.get("access") or None,
            owner=raw_config.get("owner") or None,
            group=raw_config.get("group") or None,
        )
        if model_config.materialized == "incremental":
            model_config.incremental = self._build_incremental_config(raw_config)
        if not any(attr.astuple(model_config)):
            return None

        return DbtModelDatasetFacet(config=model_config)

    def _build_incremental_config(self, raw_config: dict) -> DbtIncrementalConfig:
        """Read the incremental rebuild config from a resolved dbt node ``config``.

        Always returns a config object so the caller can use its presence to mark a model
        incremental, even when no individual field is set.
        """
        strategy = raw_config.get("incremental_strategy") or None
        incremental = DbtIncrementalConfig(
            strategy=strategy,
            unique_key=self._to_string_list(raw_config.get("unique_key")),
            # incremental_predicates is the cross-adapter key; predicates is a Spark alias.
            incremental_predicates=(
                self._to_string_list(raw_config.get("incremental_predicates"))
                or self._to_string_list(raw_config.get("predicates"))
            ),
            on_schema_change=raw_config.get("on_schema_change") or None,
            partition_by=self._parse_partition_by(raw_config.get("partition_by")),
        )
        # Microbatch parameters apply only to the microbatch strategy. dbt resolves defaults
        # (notably lookback=1) onto every model, so gate them to avoid emitting a phantom
        # time window on merge/insert_overwrite models.
        if strategy == "microbatch":
            incremental.event_time = raw_config.get("event_time") or None
            incremental.batch_size = raw_config.get("batch_size") or None
            incremental.begin = raw_config.get("begin") or None
            lookback = raw_config.get("lookback")
            # bool is an int subclass; reject it so a stray true/false is not read as a count.
            if isinstance(lookback, int) and not isinstance(lookback, bool):
                incremental.lookback = lookback
        full_refresh = raw_config.get("full_refresh")
        if isinstance(full_refresh, bool):
            incremental.full_refresh = full_refresh
        return incremental

    @staticmethod
    def _full_refresh_from_args(args: dict) -> bool | None:
        """Read the run-wide --full-refresh flag from run_results.json args.

        Returns ``None`` when the flag is absent so the facet can omit it rather than
        report a default the invocation never set.
        """
        value = args.get("full_refresh")
        return value if isinstance(value, bool) else None

    @staticmethod
    def _to_string_list(raw: Any) -> list[str] | None:
        """Normalize a dbt config value (single string, list, or None) into a list of strings.

        Empty and non-string elements are dropped; returns ``None`` when nothing remains.
        """
        if isinstance(raw, str):
            return [raw] if raw else None
        if isinstance(raw, (list, tuple)):
            values = [element for element in raw if isinstance(element, str) and element]
            return values or None
        return None

    @staticmethod
    def _parse_partition_by(raw: Any) -> DbtPartitionBy | None:
        """Normalize dbt's polymorphic ``partition_by`` config.

        BigQuery uses an object ({field, data_type, granularity}); Spark and other adapters use a
        single column or a list of columns. Returns ``None`` when nothing is set.
        """
        if isinstance(raw, dict):
            partition = DbtPartitionBy(
                field=raw.get("field") or None,
                data_type=raw.get("data_type") or None,
                granularity=raw.get("granularity") or None,
            )
            if not any(attr.astuple(partition)):
                return None
            return partition
        columns = DbtArtifactProcessor._to_string_list(raw)
        return DbtPartitionBy(columns=columns) if columns else None

    @staticmethod
    def _meta_tag_value(value: Any) -> str:
        """Render a dbt ``meta`` value as a tag value string.

        Booleans use dbt's lowercase ``true``/``false`` (matching how plain dbt tags are emitted),
        other scalars are stringified, and nested structures fall back to compact JSON.
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return value
        if isinstance(value, (int, float)):
            return str(value)
        return json.dumps(value)

    def _build_tags_run_facet(self, tags: list | None, meta: dict | None) -> tags_run.TagsRunFacet | None:
        """Build a TagsRunFacet from a dbt node's ``tags`` and ``meta``.

        Plain dbt ``tags`` keep ``source="DBT"`` and value ``"true"``; each ``meta`` entry becomes a
        key/value tag with ``source="DBT_META"``. Returns ``None`` when there is nothing to emit.
        """
        fields = [tags_run.TagsRunFacetFields(key=tag, value="true", source="DBT") for tag in (tags or [])]
        fields += [
            tags_run.TagsRunFacetFields(key=key, value=self._meta_tag_value(value), source="DBT_META")
            for key, value in (meta or {}).items()
        ]
        return tags_run.TagsRunFacet(tags=fields) if fields else None

    @staticmethod
    def extract_metadata_fields(columns: list[dict]) -> list[schema_dataset.SchemaDatasetFacetFields]:
        """
        Extract table field info from metadata's node column info
        Should be used only in the lack of catalog's presence, as there's less
        information in metadata file than in catalog.
        """
        fields = []
        for field in columns:
            fields.append(
                schema_dataset.SchemaDatasetFacetFields(
                    name=field["name"],
                    type=field.get("data_type", None),
                    description=field.get("description", None),
                )
            )
        return fields

    @staticmethod
    def extract_catalog_fields(
        columns: list[dict], metadata_columns: dict
    ) -> list[schema_dataset.SchemaDatasetFacetFields]:
        """Extract table field info from catalog's node column info"""
        fields = []
        for field in columns:
            name = field["name"]
            type_ = field.get("type", None)
            assert isinstance(type_, str), f"Catalog field {field} type is null"
            description = get_from_nullable_chain(metadata_columns, [name, "description"])
            fields.append(
                schema_dataset.SchemaDatasetFacetFields(name=name, type=type_, description=description)
            )
        return fields

    def extract_adapter_type(self, profile: dict):
        try:
            self.adapter_type = Adapter[profile["type"].upper()]
        except KeyError:
            raise NotImplementedError(
                f"Only {Adapter.adapters()} adapters are supported right now. Passed {profile['type']}"
            )

    def extract_dataset_namespace(self, profile: dict):
        self.dataset_namespace = self.extract_namespace(profile)

    def extract_namespace(self, profile: dict) -> str:
        """Extract namespace from profile's type"""
        if self.adapter_type == Adapter.SNOWFLAKE:
            return f"snowflake://{fix_account_name(profile['account'])}"
        elif self.adapter_type == Adapter.BIGQUERY:
            return "bigquery"
        elif self.adapter_type == Adapter.REDSHIFT:
            return f"redshift://{profile['host']}:{profile['port']}"
        elif self.adapter_type == Adapter.POSTGRES:
            return f"postgres://{profile['host']}:{profile['port']}"
        elif self.adapter_type == Adapter.CLICKHOUSE:
            return f"clickhouse://{profile['host']}:{profile['port']}"
        elif self.adapter_type == Adapter.TRINO:
            return f"trino://{profile['host']}:{profile['port']}"
        elif self.adapter_type == Adapter.DATABRICKS:
            return f"databricks://{profile['host']}"
        elif self.adapter_type == Adapter.SQLSERVER:
            return f"mssql://{profile['server']}:{profile['port']}"
        elif self.adapter_type == Adapter.FABRIC:
            if "port" in profile:
                return f"fabric-warehouse://{profile['server']}:{profile['port']}"
            return f"fabric-warehouse://{profile['server']}"
        elif self.adapter_type == Adapter.DREMIO:
            return f"dremio://{profile['software_host']}:{profile['port']}"
        elif self.adapter_type == Adapter.ATHENA:
            return f"awsathena://athena.{profile['region_name']}.amazonaws.com"
        elif self.adapter_type == Adapter.GLUE:
            if "account_id" in profile:
                return f"arn:aws:glue:{profile['region']}:{profile['account_id']}"
            else:
                # derive account_id from role_arn (arn:aws:iam::account_id:role/role_name)
                return f"arn:aws:glue:{profile['region']}:{profile['role_arn'].split(':')[4]}"
        elif self.adapter_type == Adapter.DUCKDB:
            return f"duckdb://{profile['path']}"
        elif self.adapter_type == Adapter.SPARK:
            port = ""

            if "port" in profile:
                port = f":{profile['port']}"
            elif profile["method"] in [
                SparkConnectionMethod.HTTP.value,
                SparkConnectionMethod.ODBC.value,
            ]:
                port = ":443"
            elif profile["method"] == SparkConnectionMethod.THRIFT.value:
                port = ":10001"

            if profile["method"] in SparkConnectionMethod.methods():
                return f"spark://{profile['host']}{port}"
            else:
                raise NotImplementedError(
                    f"Connection method `{profile['method']}` is not supported for spark adapter."
                )
        else:
            raise NotImplementedError(
                f"Only {Adapter.adapters()} adapters are supported right now. Passed {profile['type']}"
            )

    def extract_dialect(self) -> str | None:
        if not self.adapter_type:
            return None
        return self.adapter_type.value.lower()

    def get_run(
        self, run_id: str, query_id: str | None = None, run_facets: dict[str, Any] | None = None
    ) -> Run:
        if run_facets is None:
            run_facets = {}

        run_facets.update(
            {
                **self.dbt_version_facet(),
                **self.dbt_run_run_facet(),
                **self.processing_engine_facet(),
            }
        )
        if self._dbt_run_metadata:
            run_facets["parent"] = self._dbt_run_metadata.to_openlineage()

        if query_id:
            run_facets["externalQuery"] = external_query_run.ExternalQueryRunFacet(
                externalQueryId=query_id, source=self.dataset_namespace
            )

        return Run(
            runId=run_id,
            facets=run_facets,
        )

    # TODO: remove after deprecation period
    def dbt_version_facet(self) -> dict[str, DbtVersionRunFacet]:
        dbt_version = self.run_metadata.get("dbt_version")
        if not dbt_version:
            return {}

        self.logger.debug(
            "dbt_version facet is deprecated, and will be removed in future versions. "
            "Use processing_engine facet instead."
        )
        return {"dbt_version": DbtVersionRunFacet(version=dbt_version)}

    @abstractmethod
    def dbt_run_run_facet(self) -> dict[str, DbtRunRunFacet]: ...

    def _exposures_manifest(self) -> dict:
        """Manifest to read ``exposures`` from (overridden where it lives elsewhere)."""
        return self.manifest

    @cached_property
    def _exposures_by_node_id(self) -> dict[str, list[DbtExposure]]:
        """Exposures indexed by each node id in their ``depends_on.nodes``.

        Lets a model's input/output datasets look up the exposures that consume them.
        """
        raw_exposures = self._exposures_manifest().get("exposures") or {}
        index: dict[str, list[DbtExposure]] = collections.defaultdict(list)
        for raw_exposure in raw_exposures.values():
            exposure = DbtExposure(
                unique_id=raw_exposure.get("unique_id"),
                name=raw_exposure.get("name"),
                type=raw_exposure.get("type"),
                url=raw_exposure.get("url"),
            )
            for node_id in get_from_nullable_chain(raw_exposure, ["depends_on", "nodes"]) or []:
                index[node_id].append(exposure)
        return dict(index)

    def _exposures_facets(self, node_id: str | None) -> dict[str, DbtExposuresDatasetFacet]:
        """Exposures dataset facet for the dataset with this node id, or empty if none.

        Applies to both a model's output and its inputs; the input case covers
        source-backed exposures, since a source only ever appears as an input.
        """
        exposures = self._exposures_by_node_id.get(node_id) if node_id else None
        return {"dbt_exposures": DbtExposuresDatasetFacet(exposures=exposures)} if exposures else {}

    def processing_engine_facet(self) -> dict[str, processing_engine_run.ProcessingEngineRunFacet]:
        dbt_version = self.run_metadata.get("dbt_version")
        if not dbt_version:
            return {}
        return {
            "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                name="dbt",
                version=dbt_version,
                openlineageAdapterVersion=openlineage_version,
            )
        }

    @staticmethod
    def get_timings(timings: list[dict]) -> tuple[str, str]:
        """Extract timing info from run_result's timing dict"""
        try:
            timing = list(filter(lambda x: x["name"] == "execute", timings))[0]
            return timing["started_at"], timing["completed_at"]
        except IndexError:
            # Run failed: there is no timing data
            timing_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
            return timing_str, timing_str

    @staticmethod
    def removeprefix(string: str, prefix: str) -> str:
        if string.startswith(prefix):
            return string[len(prefix) :]
        else:
            return string[:]

    def get_column_lineage(
        self, namespace: str, compiled_sql: str
    ) -> column_lineage_dataset.ColumnLineageDatasetFacet | None:
        """Parse SQL and extract column-level lineage information

        Args:
            namespace: The namespace for the column lineage
            compiled_sql: The compiled SQL to parse

        Returns:
            ColumnLineageDatasetFacet if lineage can be parsed, None otherwise
        """
        try:
            parsed = parse_sql([compiled_sql])
            fields = {}
            if parsed:
                for cll_item in parsed.column_lineage:
                    fields[cll_item.descendant.name] = column_lineage_dataset.Fields(
                        inputFields=[
                            column_lineage_dataset.InputField(
                                namespace=namespace,
                                name=self._format_dataset_name(
                                    column_meta.origin.database,
                                    column_meta.origin.schema,
                                    column_meta.origin.name,
                                ),
                                field=column_meta.name,
                            )
                            for column_meta in cll_item.lineage
                            if column_meta.origin
                        ],
                    )
            if fields:
                return column_lineage_dataset.ColumnLineageDatasetFacet(fields=fields)
        except Exception as e:
            self.logger.warning(f"Failed to parse column lineage: {e}")
        return None
