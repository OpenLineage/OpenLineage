# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import collections
import datetime
import logging
from abc import abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple

import attr
from openlineage.client.event_v2 import Dataset, InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    DatasetFacet,
    InputDatasetFacet,
    JobFacet,
    OutputDatasetFacet,
    column_lineage_dataset,
    data_quality_assertions_dataset,
    datasource_dataset,
    documentation_dataset,
    job_type_job,
    output_statistics_output_dataset,
    processing_engine_run,
    schema_dataset,
    sql_job,
)
from openlineage.client.generated.external_query_run import ExternalQueryRunFacet
from openlineage.client.uuid import generate_new_uuid
from openlineage.common.provider.dbt.facets import DbtRunRunFacet, DbtVersionRunFacet, ParentRunMetadata
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


@attr.define
class ModelNode:
    metadata_node: Dict
    catalog_node: Optional[Dict] = None


@attr.define
class DbtRun:
    started_at: str
    completed_at: str
    status: str
    inputs: List[Dataset]
    output: Optional[Dataset]
    job_name: str
    namespace: str
    run_id: str = attr.field(factory=lambda: str(generate_new_uuid()))


@attr.define
class DbtRunResult:
    start: RunEvent
    complete: Optional[RunEvent] = None
    fail: Optional[RunEvent] = None


@attr.define
class DbtEvents:
    starts: List[RunEvent] = attr.field(factory=list)
    completes: List[RunEvent] = attr.field(factory=list)
    fails: List[RunEvent] = attr.field(factory=list)

    def events(self):
        return self.starts + self.completes + self.fails

    def add(self, item: Optional[DbtRunResult]):
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
    manifest: Dict
    run_results: Dict
    catalog: Optional[Dict] = None


class DbtArtifactProcessor:
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        producer: str,
        job_namespace: str,
        skip_errors: bool = False,
        logger: Optional[logging.Logger] = None,
        models: Optional[Sequence[str]] = None,
        selector: Optional[str] = None,
        openlineage_job_name: Optional[str] = None,
    ):
        self.producer = producer
        self._dbt_run_metadata: Optional[ParentRunMetadata] = None
        self.logger = logger or logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

        self.openlineage_job_name = openlineage_job_name
        self.job_namespace = job_namespace
        self.dataset_namespace = ""
        self.skip_errors = skip_errors
        self.run_metadata: dict[str, Any] = {}
        self.command = None
        self.models = models or []
        self.selector = selector
        self.manifest_version = None
        self.adapter_type: Adapter | None = None

    @property
    def dbt_run_metadata(self):
        return self._dbt_run_metadata

    @dbt_run_metadata.setter
    def dbt_run_metadata(self, metadata: ParentRunMetadata):
        self._dbt_run_metadata = metadata

    @abstractmethod
    def get_dbt_metadata(self):
        ...

    def parse(self) -> DbtEvents:
        """
        Parse dbt manifest and run_result and produce OpenLineage events.
        """
        manifest, run_result, profile, catalog = self.get_dbt_metadata()
        self.manifest_version = self.get_schema_version(manifest)

        self.run_metadata = run_result["metadata"]
        self.command = run_result["args"]["which"]

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

    def get_query_id(self, run_result: dict[str, Any]) -> Optional[str]:
        # Validate that there is an adapter_response in the run_result
        if "adapter_response" not in run_result:
            return None

        # Default to query_id for all Adapters
        query_id_key: str = "query_id"

        # Use the adapter type to make sure the correct key is used
        if self.adapter_type == Adapter.BIGQUERY:
            query_id_key = "job_id"

        query_id: Optional[str] = run_result["adapter_response"].get(query_id_key)

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

    def parse_execution(self, context: DbtRunContext, nodes: Dict) -> DbtEvents:
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

            inputs = []
            for node in context.manifest["parent_map"][run["unique_id"]]:
                if node.startswith("model."):
                    inputs.append(
                        ModelNode(
                            nodes[node],
                            get_from_nullable_chain(context.catalog, ["nodes", node]),
                        )
                    )
                elif node.startswith("source."):
                    inputs.append(
                        ModelNode(
                            context.manifest["sources"][node],
                            get_from_nullable_chain(context.catalog, ["sources", node]),
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

            job_facets: Dict[str, JobFacet] = {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType=jobType,
                    integration="DBT",
                    processingType="BATCH",
                    producer=self.producer,
                )
            }
            if sql:
                job_facets["sql"] = sql_job.SQLJobFacet(query=sql, dialect=self.extract_dialect())

            output_dataset = self.node_to_output_dataset(
                ModelNode(
                    output_node,
                    get_from_nullable_chain(context.catalog, ["nodes", run["unique_id"]]),
                ),
                has_facets=True,
                adapter_response=run.get("adapter_response", None),
            )

            # Add column lineage if SQL is available
            if sql:
                column_lineage = self.get_column_lineage(output_dataset.namespace, sql)
                if column_lineage:
                    output_dataset.facets["columnLineage"] = column_lineage  # type: ignore

            events.add(
                self.to_openlineage_events(
                    run["status"],
                    started_at,
                    completed_at,
                    self.get_run(run_id=run_id, query_id=query_id),
                    Job(namespace=self.job_namespace, name=job_name, facets=job_facets),
                    [self.node_to_dataset(node, has_facets=True) for node in inputs],
                    output_dataset,
                )
            )
        return events

    def parse_test(self, context: DbtRunContext, nodes: Dict) -> DbtEvents:
        # The tests can have different timings, so just take current time
        started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        completed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        assertions = self.parse_assertions(context, nodes)

        events = DbtEvents()
        manifest_nodes = {**context.manifest["nodes"], **context.manifest["sources"]}
        for name, node in manifest_nodes.items():
            if not name.startswith("model.") and not name.startswith("source."):
                continue
            if len(assertions[name]) == 0:
                continue

            assertion_facet = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(
                assertions=assertions[name]
            )

            namespace, name, _, _ = self.extract_dataset_data(
                ModelNode(node), assertion_facet, has_facets=False
            )

            job_name = self._format_dataset_name(
                node["database"],
                node["schema"],
                self.removeprefix(node["unique_id"], "model."),
            ) + (".build.test" if self.command == "build" else ".test")

            job_facets: Dict[str, JobFacet] = {
                "jobType": job_type_job.JobTypeJobFacet(
                    jobType="TEST",
                    integration="DBT",
                    processingType="BATCH",
                    producer=self.producer,
                )
            }

            run_id = str(generate_new_uuid())
            dataset_facets: Dict[str, InputDatasetFacet] = {"dataQualityAssertions": assertion_facet}
            events.add(
                self.to_openlineage_events(
                    "success",
                    started_at,
                    completed_at,
                    self.get_run(run_id=run_id),
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
        return events

    def parse_assertions(
        self, context: DbtRunContext, nodes: Dict
    ) -> Dict[str, List[data_quality_assertions_dataset.Assertion]]:
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
                name = test_node["test_metadata"]["name"]
                node_columns = test_node["test_metadata"]

            assertions[model_node].append(
                data_quality_assertions_dataset.Assertion(
                    assertion=name,
                    success=True if run["status"] == "pass" else False,
                    column=get_from_nullable_chain(node_columns, ["kwargs", "column_name"]),
                )
            )

            if not model_node:
                self.logger.warning(f"Model node connected to test {nodes[run['unique_id']]} not found")
        return assertions  # type: ignore

    def to_openlineage_events(self, *args, **kwargs) -> Optional[DbtRunResult]:
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
        inputs: List[InputDataset],
        output: Optional[OutputDataset],
    ) -> Optional[DbtRunResult]:
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
        adapter_response: Optional[Dict] = None,
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

        output_facets: Dict[str, OutputDatasetFacet] = {}
        output_facets[
            "outputStatistics"
        ] = output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(
            rowCount=row_count if row_count > 0 else None,
            size=byte_count if byte_count > 0 else None,
        )
        return OutputDataset(name=name, namespace=namespace, facets=facets, outputFacets=output_facets)

    def _format_dataset_name(self, database: Optional[str], schema: Optional[str], table: str) -> str:
        return ".".join(list(filter(None, [database, schema, table])))

    def extract_dataset_data(
        self,
        node: ModelNode,
        assertions: Optional[data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet],
        has_facets: bool = False,
    ) -> Tuple[str, str, Dict, Dict]:
        facets: Dict[str, DatasetFacet]
        input_facets: Dict[str, InputDatasetFacet] = {}
        if has_facets:
            facets = {
                "dataSource": datasource_dataset.DatasourceDatasetFacet(
                    name=self.dataset_namespace, uri=self.dataset_namespace
                ),
            }

            documentation = node.metadata_node["description"]
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
        else:
            facets = {}
        return (
            self.dataset_namespace,
            self._format_dataset_name(
                node.metadata_node["database"],
                node.metadata_node["schema"],
                node.metadata_node["name"],
            ),
            facets,
            input_facets,
        )

    @staticmethod
    def extract_metadata_fields(columns: List[Dict]) -> List[schema_dataset.SchemaDatasetFacetFields]:
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
        columns: List[Dict], metadata_columns: Dict
    ) -> List[schema_dataset.SchemaDatasetFacetFields]:
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

    def extract_adapter_type(self, profile: Dict):
        try:
            self.adapter_type = Adapter[profile["type"].upper()]
        except KeyError:
            raise NotImplementedError(
                f"Only {Adapter.adapters()} adapters are supported right now. Passed {profile['type']}"
            )

    def extract_dataset_namespace(self, profile: Dict):
        self.dataset_namespace = self.extract_namespace(profile)

    def extract_namespace(self, profile: Dict) -> str:
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
                port = "443"
            elif profile["method"] == SparkConnectionMethod.THRIFT.value:
                port = "10001"

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

    def get_run(self, run_id: str, query_id: Optional[str] = None) -> Run:
        run_facets = {
            **self.dbt_version_facet(),
            **self.dbt_run_run_facet(),
            **self.processing_engine_facet(),
        }
        if self._dbt_run_metadata:
            run_facets["parent"] = self._dbt_run_metadata.to_openlineage()

        if query_id:
            run_facets["externalQuery"] = ExternalQueryRunFacet(
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

    def dbt_run_run_facet(self) -> dict[str, DbtRunRunFacet]:
        invocation_id = self.run_metadata.get("invocation_id")
        if not invocation_id:
            return {}
        return {"dbt_run": DbtRunRunFacet(invocation_id=invocation_id)}

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
    def get_timings(timings: List[Dict]) -> Tuple[str, str]:
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
    ) -> Optional[column_lineage_dataset.ColumnLineageDatasetFacet]:
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
