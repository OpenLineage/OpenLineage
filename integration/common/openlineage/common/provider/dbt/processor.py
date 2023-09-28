# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import collections
import datetime
import logging
import uuid
from abc import abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Tuple

import attr
from openlineage.client.facet import (
    Assertion,
    BaseFacet,
    DataQualityAssertionsDatasetFacet,
    DataSourceDatasetFacet,
    OutputStatisticsOutputDatasetFacet,
    ParentRunFacet,
    SchemaDatasetFacet,
    SchemaField,
    SqlJobFacet,
)
from openlineage.client.run import Dataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.common.provider.snowflake import fix_account_name
from openlineage.common.schema import GITHUB_LOCATION
from openlineage.common.utils import get_from_multiple_chains, get_from_nullable_chain


class Adapter(Enum):
    # This class represents supported adapters.
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    SPARK = "spark"
    POSTGRES = "postgres"
    DATABRICKS = "databricks"
    SQLSERVER = "sqlserver"

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


@attr.s
class ModelNode:
    metadata_node: Dict = attr.ib()
    catalog_node: Optional[Dict] = attr.ib(default=None)


@attr.s
class DbtRun:
    started_at: str = attr.ib()
    completed_at: str = attr.ib()
    status: str = attr.ib()
    inputs: List[Dataset] = attr.ib()
    output: Optional[Dataset] = attr.ib()
    job_name: str = attr.ib()
    namespace: str = attr.ib()
    run_id: str = attr.ib(factory=lambda: str(uuid.uuid4()))


@attr.s
class DbtRunResult:
    start: RunEvent = attr.ib()
    complete: Optional[RunEvent] = attr.ib(default=None)
    fail: Optional[RunEvent] = attr.ib(default=None)


@attr.s
class DbtEvents:
    starts: List[RunEvent] = attr.ib(factory=list)
    completes: List[RunEvent] = attr.ib(factory=list)
    fails: List[RunEvent] = attr.ib(factory=list)

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
        raise NotImplementedError()


@attr.s
class DbtRunContext:
    manifest: Dict = attr.ib()
    run_results: Dict = attr.ib()
    catalog: Optional[Dict] = attr.ib(default=None)


@attr.s
class ParentRunMetadata:
    run_id: str = attr.ib()
    job_name: str = attr.ib()
    job_namespace: str = attr.ib()

    def to_openlineage(self) -> ParentRunFacet:
        return ParentRunFacet.create(
            runId=self.run_id, name=self.job_name, namespace=self.job_namespace
        )


@attr.s
class DbtVersionRunFacet(BaseFacet):
    version: str = attr.ib()

    @staticmethod
    def _get_schema() -> str:
        return GITHUB_LOCATION + "dbt-version-run-facet.json"



class DbtArtifactProcessor:
    should_raise_on_unsupported_command = True

    def __init__(
        self,
        producer: str,
        job_namespace: str,
        skip_errors: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self.producer = producer
        self._dbt_run_metadata: Optional[ParentRunMetadata] = None
        self.logger = logger or logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )

        self.job_namespace = job_namespace
        self.dataset_namespace = ""
        self.skip_errors = skip_errors
        self.run_metadata = None
        self.command = None

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
            if any(
                name.startswith(prefix) for prefix in ("model.", "test.", "snapshot.")
            ):
                nodes[name] = node

        context = DbtRunContext(manifest, run_result, catalog)

        events = DbtEvents()

        if self.command not in ["run", "build", "test", "seed", "snapshot"]:
            if self.should_raise_on_unsupported_command:
                raise UnsupportedDbtCommand(
                    f"Not recognized run command "
                    f"{self.command} - should be run, test, seed or build"
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
        str_schema_version = get_from_nullable_chain(
            metadata, ["metadata", "dbt_schema_version"]
        )
        return cls.get_version_number(str_schema_version)

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
            if not any(
                name.startswith(prefix) for prefix in ("model.", "source.", "snapshot.")
            ):
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

            run_id = str(uuid.uuid4())
            if name.startswith("snapshot."):
                job_name = (
                    f"{output_node['database']}.{output_node['schema']}"
                    f".{self.removeprefix(run['unique_id'], 'snapshot.')}"
                    + (".build.snapshot" if self.command == "build" else ".snapshot")
                )
            else:
                job_name = (
                    f"{output_node['database']}.{output_node['schema']}"
                    f".{self.removeprefix(run['unique_id'], 'model.')}"
                    + (".build.run" if self.command == "build" else "")
                )

            if self.manifest_version >= 7:
                sql = output_node.get("compiled_code", None)
            else:
                sql = output_node["compiled_sql"]

            job_facets = {}
            if sql:
                job_facets["sql"] = SqlJobFacet(sql)

            events.add(
                self.to_openlineage_events(
                    run["status"],
                    started_at,
                    completed_at,
                    self.get_run(run_id),
                    Job(namespace=self.job_namespace, name=job_name, facets=job_facets),
                    [self.node_to_dataset(node, has_facets=True) for node in inputs],
                    self.node_to_output_dataset(
                        ModelNode(
                            output_node,
                            get_from_nullable_chain(
                                context.catalog, ["nodes", run["unique_id"]]
                            ),
                        ),
                        has_facets=True,
                    ),
                )
            )
        return events

    def parse_test(self, context: DbtRunContext, nodes: Dict) -> DbtEvents:
        # The tests can have different timings, so just take current time
        started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        completed_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        assertions = self.parse_assertions(context, nodes)

        events = DbtEvents()
        for name, node in context.manifest["nodes"].items():
            if not name.startswith("model.") and not name.startswith("source."):
                continue
            if len(assertions[name]) == 0:
                continue

            assertion_facet = DataQualityAssertionsDatasetFacet(
                assertions=assertions[name]
            )

            namespace, name, _ = self.extract_dataset_data(
                ModelNode(node), assertion_facet, has_facets=False
            )

            job_name = (
                f"{node['database']}.{node['schema']}."
                f"{self.removeprefix(node['unique_id'], 'model.')}"
                + (".build.test" if self.command == "build" else ".test")
            )

            run_id = str(uuid.uuid4())
            events.add(
                self.to_openlineage_events(
                    "success",
                    started_at,
                    completed_at,
                    self.get_run(run_id),
                    Job(self.job_namespace, job_name),
                    [
                        Dataset(
                            namespace,
                            name,
                            facets={"dataQualityAssertions": assertion_facet},
                        )
                    ],
                    None,
                )
            )
        return events

    def parse_assertions(
        self, context: DbtRunContext, nodes: Dict
    ) -> Dict[str, List[Assertion]]:
        assertions = collections.defaultdict(list)
        for run in context.run_results["results"]:
            if not run["unique_id"].startswith("test."):
                continue
            test_node = nodes[run["unique_id"]]

            model_node = None
            for node in context.manifest["parent_map"][run["unique_id"]]:
                if node.startswith("model.") or node.startswith("source."):
                    model_node = node

            assertions[model_node].append(
                Assertion(
                    assertion=test_node["test_metadata"]["name"],
                    success=True if run["status"] == "pass" else False,
                    column=get_from_nullable_chain(
                        test_node["test_metadata"], ["kwargs", "column_name"]
                    ),
                )
            )

            if not model_node:
                raise ValueError(
                    f"Model node connected to test {nodes[run['unique_id']]} not found"
                )
        return assertions  # type: ignore

    def to_openlineage_events(self, *args, **kwargs) -> Optional[DbtRunResult]:
        try:
            return self._to_openlineage_events(*args, **kwargs)
        except Exception as e:
            if self.skip_errors:
                return None
            raise ValueError() from e

    def _to_openlineage_events(
        self,
        status: str,
        started_at: str,
        completed_at: str,
        run: Run,
        job: Job,
        inputs: List[Dataset],
        output: Optional[Dataset],
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
            raise ValueError(
                f"Run status was {status}, "
                f"should be in ['success', 'skipped', 'error']"
            )

    def node_to_dataset(
        self,
        node: ModelNode,
        assertions: Optional[DataQualityAssertionsDatasetFacet] = None,
        has_facets: bool = False,
    ) -> Dataset:
        name, namespace, facets = self.extract_dataset_data(
            node, assertions, has_facets
        )
        return Dataset(name, namespace, facets)

    def node_to_output_dataset(
        self, node: ModelNode, has_facets: bool = False
    ) -> OutputDataset:
        name, namespace, facets = self.extract_dataset_data(node, None, has_facets)
        output_facets = {}
        if has_facets and node.catalog_node:
            bytes = get_from_multiple_chains(
                node.catalog_node,
                [
                    ["stats", "num_bytes", "value"],  # bigquery
                    ["stats", "bytes", "value"],  # snowflake
                    [
                        "stats",
                        "size",
                        "value",
                    ],  # redshift (Note: size = count of 1MB blocks)
                ],
            )
            rows = get_from_multiple_chains(
                node.catalog_node,
                [
                    ["stats", "num_rows", "value"],  # bigquery
                    ["stats", "row_count", "value"],  # snowflake
                    ["stats", "rows", "value"],  # redshift
                ],
            )

            if bytes:
                bytes = (
                    int(bytes)
                    if self.adapter_type != Adapter.REDSHIFT
                    else int(rows) * (2**20)
                )
            if rows:
                rows = int(rows)

                output_facets["outputStatistics"] = OutputStatisticsOutputDatasetFacet(
                    rowCount=rows, size=bytes
                )
        return OutputDataset(name, namespace, facets, output_facets)

    def extract_dataset_data(
        self,
        node: ModelNode,
        assertions: Optional[DataQualityAssertionsDatasetFacet],
        has_facets: bool = False,
    ) -> Tuple[str, str, Dict]:
        if has_facets:
            facets = {
                "dataSource": DataSourceDatasetFacet(
                    name=self.dataset_namespace, uri=self.dataset_namespace
                ),
                "schema": SchemaDatasetFacet(
                    fields=self.extract_metadata_fields(
                        node.metadata_node["columns"].values()
                    )
                ),
            }
            if assertions:
                facets["dataQualityAssertions"] = assertions
            if node.catalog_node:
                facets["schema"] = SchemaDatasetFacet(
                    fields=self.extract_catalog_fields(
                        node.catalog_node["columns"].values(),
                        node.metadata_node["columns"],
                    )
                )
        else:
            facets = {}
        return (
            self.dataset_namespace,
            f"{node.metadata_node['database']}."
            f"{node.metadata_node['schema']}."
            f"{node.metadata_node['name']}",
            facets,
        )

    @staticmethod
    def extract_metadata_fields(columns: List[Dict]) -> List[SchemaField]:
        """
        Extract table field info from metadata's node column info
        Should be used only in the lack of catalog's presence, as there's less
        information in metadata file than in catalog.
        """
        fields = []
        for field in columns:
            of_type, description = None, None
            if "data_type" in field and field["data_type"] is not None:
                of_type = field["data_type"]
            if "description" in field and field["description"] is not None:
                description = field["description"]
            fields.append(
                SchemaField(
                    name=field["name"], type=of_type or "", description=description
                )
            )
        return fields

    @staticmethod
    def extract_catalog_fields(
        columns: List[Dict], metadata_columns: Dict
    ) -> List[SchemaField]:
        """Extract table field info from catalog's node column info"""
        fields = []
        for field in columns:
            name = field["name"]
            type = None
            if "type" in field and field["type"] is not None:
                type = field["type"]
            description = get_from_nullable_chain(
                metadata_columns, [name, "description"]
            )
            assert isinstance(type, str)
            fields.append(SchemaField(name=name, type=type, description=description))
        return fields

    def extract_adapter_type(self, profile: Dict):
        try:
            self.adapter_type = Adapter[profile["type"].upper()]
        except KeyError:
            raise NotImplementedError(
                f"Only {Adapter.adapters()} adapters are supported right now. "
                f"Passed {profile['type']}"
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
        elif self.adapter_type == Adapter.DATABRICKS:
            return f"databricks://{profile['host']}"
        elif self.adapter_type == Adapter.SQLSERVER:
            return f"mssql://{profile['server']}:{profile['port']}"
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
                    f"Connection method `{profile['method']}` is not "
                    f"supported for spark adapter."
                )
        else:
            raise NotImplementedError(
                f"Only {Adapter.adapters()} adapters are supported right now. "
                f"Passed {profile['type']}"
            )

    def get_run(self, run_id: str) -> Run:
        return Run(
            runId=run_id,
            facets={
                "parent": self._dbt_run_metadata.to_openlineage()
                if self._dbt_run_metadata
                else None,
                "dbt_version": self.dbt_version_facet(),
            },
        )

    def dbt_version_facet(self):
        return DbtVersionRunFacet(version=self.run_metadata["dbt_version"])

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
