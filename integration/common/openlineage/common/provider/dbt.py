import json
import yaml
import os
import uuid
from typing import List, Tuple, Dict, Optional

import attr

from openlineage.client.facet import DataSourceDatasetFacet, SchemaDatasetFacet, SchemaField, \
    SqlJobFacet
from openlineage.common import __version__
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.common.utils import get_from_nullable_chain

PRODUCER = f"openlineage-dbt/{__version__}"


@attr.s
class DbtRun:
    started_at: str = attr.ib()
    completed_at: str = attr.ib()
    status: str = attr.ib()
    inputs: List[Dict] = attr.ib()
    output: Dict = attr.ib()
    job_name: str = attr.ib()
    namespace: str = attr.ib()
    run_id: str = attr.ib(factory=lambda: str(uuid.uuid4()))


@attr.s
class DbtEvents:
    starts: List[RunEvent] = attr.ib()
    completes: List[RunEvent] = attr.ib()


class DbtArtifactProcessor:
    def __init__(self, project: str = 'dbt_project.yaml'):
        self.dir = os.path.abspath(os.path.dirname(project))
        self.project = self.load_yaml(project)
        self.namespace = ""

    def parse(self, target: Optional[str] = None) -> DbtEvents:
        manifest = self.load_manifest(
            os.path.join(self.dir, self.project['target-path'], 'manifest.json')
        )
        run_result = self.load_run_results(
            os.path.join(self.dir, self.project['target-path'], 'run_results.json')
        )

        profile_dir = run_result['args']['profiles_dir']

        profile = self.load_yaml(
            os.path.join(profile_dir, 'profiles.yaml')
        )[self.project['profile']]

        if target:
            profile = profile['outputs'][target]
        else:
            profile = profile['outputs'][profile['target']]

        self.set_namespace(profile)

        runs = self.parse_artifacts(manifest, run_result)

        start_events, end_events = [], []
        for run in runs:
            start, end = self.to_openlineage_events(run)
            start_events.append(start)
            end_events.append(end)
        return DbtEvents(start_events, end_events)

    @staticmethod
    def load_manifest(path: str) -> Dict:
        with open(path, 'r') as f:
            manifest = json.load(f)
            schema_version = get_from_nullable_chain(manifest, ['metadata', 'dbt_schema_version'])
            if schema_version != "https://schemas.getdbt.com/dbt/manifest/v2.json":
                # Maybe we should accept it and throw exception only if it substantially differs
                raise ValueError(f"Wrong version of dbt metadata manifest: {schema_version}")
            return manifest

    @staticmethod
    def load_run_results(path: str) -> Dict:
        with open(path, 'r') as f:
            run_results = json.load(f)
            schema_version = get_from_nullable_chain(
                run_results, ['metadata', 'dbt_schema_version']
            )
            if schema_version != "https://schemas.getdbt.com/dbt/run-results/v2.json":
                # Maybe we should accept it and throw exception only if it substantially differs
                raise ValueError(f"Wrong version of dbt run_results manifest: {schema_version}")
            return run_results

    @staticmethod
    def load_yaml(path: str) -> Dict:
        with open(path, 'r') as f:
            return yaml.load(f)

    def parse_artifacts(self, manifest: Dict, run_results: Dict) -> List[DbtRun]:
        nodes = {}
        runs = []

        # Filter non-model nodes
        for name, node in manifest['nodes'].items():
            if name.startswith('model.'):
                nodes[name] = node

        for run in run_results['results']:

            def get_timings(timings: List[Dict]) -> Tuple[str, str]:
                timing = list(filter(lambda x: x['name'] == 'execute', timings))[0]
                return timing['started_at'], timing['completed_at']

            started_at, completed_at = get_timings(run['timing'])
            inputs = [
                nodes[node]
                for node
                in manifest['parent_map'][run['unique_id']]
                if node.startswith('model.')
            ] + [
                manifest['sources'][source]
                for source
                in manifest['parent_map'][run['unique_id']]
                if source.startswith('source.')
            ]

            runs.append(DbtRun(
                started_at,
                completed_at,
                run['status'],
                inputs,
                nodes[run['unique_id']],
                run['unique_id'],
                self.namespace
            ))
        return runs

    def to_openlineage_events(self, run: DbtRun) -> Tuple[RunEvent, RunEvent]:
        try:
            return RunEvent(
                eventType=RunState.START,
                eventTime=run.started_at,
                run=Run(
                    runId=run.run_id
                ),
                job=Job(
                    namespace=self.namespace,
                    name=run.job_name
                ),
                producer=PRODUCER,
                inputs=[self.node_to_dataset(node) for node in run.inputs],
                outputs=[self.node_to_dataset(run.output)]
            ), RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=run.completed_at,
                run=Run(
                    runId=run.run_id
                ),
                job=Job(
                    namespace=self.namespace,
                    name=run.job_name,
                    facets={
                        'sql': SqlJobFacet(run.output['compiled_sql'])
                    }
                ),
                producer=PRODUCER,
                inputs=[self.node_to_dataset(node, facets=True) for node in run.inputs],
                outputs=[self.node_to_dataset(run.output, facets=True)]
            )
        except Exception as e:
            raise ValueError(e)

    def node_to_dataset(self, node: Dict, facets: bool = False) -> Dataset:
        if facets:
            facets = {
                'dataSource': DataSourceDatasetFacet(
                    name=self.namespace,
                    uri=self.namespace
                ),
                'schema': SchemaDatasetFacet(
                    fields=[SchemaField(
                        name=field['name'], type=field['data_type']
                    ) for field in node['columns'].values()]
                )
            }
        else:
            facets = {}
        return Dataset(
            namespace=self.namespace,
            name=f"{node['database']}.{node['schema']}.{node['name']}",
            facets=facets
        )

    def set_namespace(self, profile: Dict):
        if profile['type'] == 'snowflake':
            self.namespace = f"snowflake://{profile['account']}"
        elif profile['type'] == 'bigquery':
            self.namespace = "bigquery"
        else:
            raise NotImplementedError(
                "Only 'snowflake' and 'bigquery' adapters are supported right now."
            )
