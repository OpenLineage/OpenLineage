import datetime
import json
import yaml
import os
import uuid
from typing import List, Tuple, Dict, Optional

import attr

from openlineage.client.facet import DataSourceDatasetFacet, SchemaDatasetFacet, SchemaField, \
    SqlJobFacet, OutputStatisticsOutputDatasetFacet
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset, OutputDataset
from openlineage.common.utils import get_from_nullable_chain, get_from_multiple_chains


@attr.s
class ModelNode:
    metadata_node: Dict = attr.ib()
    catalog_node: Optional[Dict] = attr.ib(default=None)


@attr.s
class DbtRun:
    started_at: str = attr.ib()
    completed_at: str = attr.ib()
    status: str = attr.ib()
    inputs: List[ModelNode] = attr.ib()
    output: ModelNode = attr.ib()
    job_name: str = attr.ib()
    namespace: str = attr.ib()
    run_id: str = attr.ib(factory=lambda: str(uuid.uuid4()))


@attr.s
class DbtEvents:
    starts: List[RunEvent] = attr.ib()
    completes: List[RunEvent] = attr.ib()
    fails: List[RunEvent] = attr.ib()

    def events(self):
        return self.starts + self.completes + self.fails


@attr.s
class DbtRunResult:
    start: RunEvent = attr.ib()
    complete: Optional[RunEvent] = attr.ib(default=None)
    fail: Optional[RunEvent] = attr.ib(default=None)


class DbtArtifactProcessor:
    def __init__(
        self,
        producer: str,
        project_dir: Optional[str] = None,
        profile_name: Optional[str] = None,
        target: Optional[str] = None,
        skip_errors: bool = False
    ):
        self.producer = producer
        self.dir = os.path.abspath(project_dir)
        self.profile_name = profile_name
        self.target = target
        self.project = self.load_yaml(os.path.join(project_dir, 'dbt_project.yml'))
        self.job_namespace = ""
        self.dataset_namespace = ""
        self.skip_errors = skip_errors

    def parse(self) -> DbtEvents:
        """
            Parse dbt manifest and run_result and produce OpenLineage events.
        """
        manifest = self.load_manifest(
            os.path.join(self.dir, self.project['target-path'], 'manifest.json')
        )
        run_result = self.load_run_results(
            os.path.join(self.dir, self.project['target-path'], 'run_results.json')
        )
        catalog = self.load_catalog(
            os.path.join(self.dir, self.project['target-path'], 'catalog.json')
        )

        profile_dir = run_result['args']['profiles_dir']

        if not self.profile_name:
            self.profile_name = self.project['profile']

        profile = self.load_yaml(
            os.path.join(profile_dir, 'profiles.yml')
        )[self.profile_name]

        if not self.target:
            self.target = profile['target']

        profile = profile['outputs'][self.target]

        self.extract_dataset_namespace(profile)
        self.extract_job_namespace(profile)

        runs = self.parse_artifacts(manifest, run_result, catalog)

        start_events, complete_events, fail_events = [], [], []
        for run in runs:
            results = self.to_openlineage_events(run)
            if not results:
                continue
            start_events.append(results.start)
            if results.complete:
                complete_events.append(results.complete)
            elif results.fail:
                fail_events.append(results.fail)
        return DbtEvents(start_events, complete_events, fail_events)

    @staticmethod
    def load_metadata(path: str, desired_schema_version: str) -> Dict:
        with open(path, 'r') as f:
            metadata = json.load(f)
            schema_version = get_from_nullable_chain(metadata, ['metadata', 'dbt_schema_version'])
            if schema_version != desired_schema_version:
                # Maybe we should accept it and throw exception only if it substantially differs
                raise ValueError(f"Wrong version of dbt metadata: {schema_version}, "
                                 f"should be {desired_schema_version}")
            return metadata

    @classmethod
    def load_manifest(cls, path: str) -> Dict:
        return cls.load_metadata(path, "https://schemas.getdbt.com/dbt/manifest/v2.json")

    @classmethod
    def load_run_results(cls, path: str) -> Dict:
        return cls.load_metadata(path, "https://schemas.getdbt.com/dbt/run-results/v2.json")

    @classmethod
    def load_catalog(cls, path: str) -> Optional[Dict]:
        try:
            return cls.load_metadata(path, "https://schemas.getdbt.com/dbt/catalog/v1.json")
        except FileNotFoundError:
            return None

    @staticmethod
    def load_yaml(path: str) -> Dict:
        with open(path, 'r') as f:
            return yaml.load(f, Loader=yaml.FullLoader)

    def parse_artifacts(
            self,
            manifest: Dict,
            run_results: Dict,
            catalog: Optional[Dict]
    ) -> List[DbtRun]:
        nodes = {}
        runs = []

        # Filter non-model nodes
        for name, node in manifest['nodes'].items():
            if name.startswith('model.'):
                nodes[name] = node

        for run in run_results['results']:

            def get_timings(timings: List[Dict]) -> Tuple[str, str]:
                try:
                    timing = list(filter(lambda x: x['name'] == 'execute', timings))[0]
                    return timing['started_at'], timing['completed_at']
                except IndexError:
                    # Run failed: there is no timing data
                    timing = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    return timing, timing

            started_at, completed_at = get_timings(run['timing'])

            inputs = []
            for node in manifest['parent_map'][run['unique_id']]:
                if node.startswith('model.'):
                    inputs.append(ModelNode(
                        nodes[node],
                        get_from_nullable_chain(catalog, ['nodes', node])
                    ))
                elif node.startswith('source.'):
                    inputs.append(ModelNode(
                        manifest['sources'][node],
                        get_from_nullable_chain(catalog, ['sources', node])
                    ))

            output_node = nodes[run['unique_id']]

            runs.append(DbtRun(
                started_at,
                completed_at,
                run['status'],
                inputs,
                ModelNode(
                    output_node,
                    get_from_nullable_chain(catalog, ['nodes', run['unique_id']])
                ),
                f"{output_node['database']}."
                f"{output_node['schema']}."
                f"{self.removeprefix(run['unique_id'], 'model.')}",
                self.dataset_namespace
            ))
        return runs

    def to_openlineage_events(self, run: DbtRun) -> Optional[DbtRunResult]:
        try:
            return self._to_openlineage_events(run)
        except Exception as e:
            if self.skip_errors:
                return None
            raise ValueError(e)

    def _to_openlineage_events(self, run: DbtRun) -> Optional[DbtRunResult]:
        if run.status == 'skipped':
            return None

        start = RunEvent(
            eventType=RunState.START,
            eventTime=run.started_at,
            run=Run(
                runId=run.run_id
            ),
            job=Job(
                namespace=self.job_namespace,
                name=run.job_name
            ),
            producer=self.producer,
            inputs=[self.node_to_dataset(node) for node in run.inputs],
            outputs=[self.node_to_output_dataset(run.output)]
        )

        if run.status == 'success':
            return DbtRunResult(
                start,
                complete=RunEvent(
                    eventType=RunState.COMPLETE,
                    eventTime=run.completed_at,
                    run=Run(
                        runId=run.run_id
                    ),
                    job=Job(
                        namespace=self.job_namespace,
                        name=run.job_name,
                        facets={
                            'sql': SqlJobFacet(run.output.metadata_node['compiled_sql'])
                        }
                    ),
                    producer=self.producer,
                    inputs=[self.node_to_dataset(node, has_facets=True) for node in run.inputs],
                    outputs=[self.node_to_output_dataset(run.output, has_facets=True)]
                )
            )
        elif run.status == 'error':
            return DbtRunResult(
                start,
                fail=RunEvent(
                    eventType=RunState.FAIL,
                    eventTime=run.completed_at,
                    run=Run(
                        runId=run.run_id
                    ),
                    job=Job(
                        namespace=self.job_namespace,
                        name=run.job_name,
                        facets={
                            'sql': SqlJobFacet(run.output.metadata_node['compiled_sql'])
                        }
                    ),
                    producer=self.producer,
                    inputs=[self.node_to_dataset(node, has_facets=True) for node in run.inputs],
                    outputs=[]
                )
            )
        else:
            # Should not happen?
            raise ValueError(f"Run status was {run.status}, "
                             f"should be in ['success', 'skipped', 'skipped']")

    def extract_dataset_data(
            self, node: ModelNode, has_facets: bool = False
    ) -> Tuple[str, str, Dict]:
        if has_facets:
            facets = {
                'dataSource': DataSourceDatasetFacet(
                    name=self.dataset_namespace,
                    uri=self.dataset_namespace
                ),
                'schema': SchemaDatasetFacet(
                    fields=self.extract_metadata_fields(node.metadata_node['columns'].values())
                )
            }
            if node.catalog_node:
                facets['schema'] = SchemaDatasetFacet(
                    fields=self.extract_catalog_fields(node.catalog_node['columns'].values())
                )
        else:
            facets = {}
        return (
            self.dataset_namespace,
            f"{node.metadata_node['database']}."
            f"{node.metadata_node['schema']}."
            f"{node.metadata_node['name']}",
            facets
        )

    def node_to_dataset(self, node: ModelNode, has_facets: bool = False) -> Dataset:
        return Dataset(
            *self.extract_dataset_data(node, has_facets)
        )

    def node_to_output_dataset(self, node: ModelNode, has_facets: bool = False) -> OutputDataset:
        name, namespace, facets = self.extract_dataset_data(node, has_facets)
        output_facets = {}
        if has_facets and node.catalog_node:
            bytes = get_from_multiple_chains(
                node.catalog_node,
                [
                    ['stats', 'num_bytes', 'value'],  # bigquery
                    ['stats', 'bytes', 'value']  # snowflake
                ]
            )
            rows = get_from_multiple_chains(
                node.catalog_node,
                [
                    ['stats', 'num_rows', 'value'],  # bigquery
                    ['stats', 'row_count', 'value']  # snowflake
                ]
            )

            if bytes:
                bytes = int(bytes)
            if rows:
                rows = int(rows)

                output_facets['outputStatistics'] = OutputStatisticsOutputDatasetFacet(
                    rowCount=rows,
                    size=bytes
                )
        return OutputDataset(
            name, namespace, facets, output_facets
        )

    @staticmethod
    def extract_metadata_fields(columns: List[Dict]) -> List[SchemaField]:
        fields = []
        for field in columns:
            type = None
            if 'data_type' in field and field['data_type'] is not None:
                type = field['data_type']
            fields.append(SchemaField(
                name=field['name'], type=type
            ))
        return fields

    @staticmethod
    def extract_catalog_fields(columns: List[Dict]) -> List[SchemaField]:
        fields = []
        for field in columns:
            type, description = None, None
            if 'type' in field and field['type'] is not None:
                type = field['type']
            if 'column' in field and field['column'] is not None:
                description = field['column']
            fields.append(SchemaField(
                name=field['name'], type=type, description=description
            ))
        return fields

    def extract_dataset_namespace(self, profile: Dict):
        self.dataset_namespace = self.extract_namespace(profile)

    def extract_job_namespace(self, profile: Dict):
        self.job_namespace = os.environ.get(
            'OPENLINEAGE_NAMESPACE',
            self.extract_namespace(profile)
        )

    def extract_namespace(self, profile: Dict) -> str:
        if profile['type'] == 'snowflake':
            return f"snowflake://{profile['account']}"
        elif profile['type'] == 'bigquery':
            return "bigquery"
        else:
            raise NotImplementedError(
                f"Only 'snowflake' and 'bigquery' adapters are supported right now. "
                f"Passed {profile['type']}"
            )

    @staticmethod
    def removeprefix(string: str, prefix: str) -> str:
        if string.startswith(prefix):
            return string[len(prefix):]
        else:
            return string[:]
