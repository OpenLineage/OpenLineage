import datetime
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
    fails: List[RunEvent] = attr.ib()

    def events(self):
        return self.starts + self.completes + self.fails


@attr.s
class DbtRunResult:
    start: RunEvent = attr.ib()
    complete: Optional[RunEvent] = attr.ib(default=None)
    fail: Optional[RunEvent] = attr.ib(default=None)


class DbtArtifactProcessor:
    def __init__(self, project: str = 'dbt_project.yml', skip_errors: bool = False):
        self.dir = os.path.abspath(os.path.dirname(project))
        self.project = self.load_yaml(project)
        self.namespace = ""
        self.skip_errors = skip_errors

    def parse(self, target: Optional[str] = None) -> DbtEvents:
        """
            Parse dbt manifest and run_result and produce OpenLineage events.
        """
        manifest = self.load_manifest(
            os.path.join(self.dir, self.project['target-path'], 'manifest.json')
        )
        run_result = self.load_run_results(
            os.path.join(self.dir, self.project['target-path'], 'run_results.json')
        )

        profile_dir = run_result['args']['profiles_dir']

        profile = self.load_yaml(
            os.path.join(profile_dir, 'profiles.yml')
        )[self.project['profile']]

        if target:
            profile = profile['outputs'][target]
        else:
            profile = profile['outputs'][profile['target']]

        self.extract_namespace(profile)

        runs = self.parse_artifacts(manifest, run_result)

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
            return yaml.load(f, Loader=yaml.FullLoader)

    def parse_artifacts(self, manifest: Dict, run_results: Dict) -> List[DbtRun]:
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
                namespace=self.namespace,
                name=run.job_name
            ),
            producer=PRODUCER,
            inputs=[self.node_to_dataset(node) for node in run.inputs],
            outputs=[self.node_to_dataset(run.output)]
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
                        namespace=self.namespace,
                        name=run.job_name,
                        facets={
                            'sql': SqlJobFacet(run.output['compiled_sql'])
                        }
                    ),
                    producer=PRODUCER,
                    inputs=[self.node_to_dataset(node, has_facets=True) for node in run.inputs],
                    outputs=[self.node_to_dataset(run.output, has_facets=True)]
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
                        namespace=self.namespace,
                        name=run.job_name,
                        facets={
                            'sql': SqlJobFacet(run.output['compiled_sql'])
                        }
                    ),
                    producer=PRODUCER,
                    inputs=[self.node_to_dataset(node, has_facets=True) for node in run.inputs],
                    outputs=[]
                )
            )
        else:
            # Should not happen?
            raise ValueError(f"Run status was {run.status}, "
                             f"should be in ['success', 'skipped', 'skipped']")

    def node_to_dataset(self, node: Dict, has_facets: bool = False) -> Dataset:
        if has_facets:
            has_facets = {
                'dataSource': DataSourceDatasetFacet(
                    name=self.namespace,
                    uri=self.namespace
                ),
                'schema': SchemaDatasetFacet(
                    fields=self.extract_fields(node['columns'].values())
                )
            }
        else:
            has_facets = {}
        return Dataset(
            namespace=self.namespace,
            name=f"{node['database']}.{node['schema']}.{node['name']}",
            facets=has_facets
        )

    def extract_fields(self, columns: List[Dict]) -> List[SchemaField]:
        fields = []
        for field in columns:
            type = None
            if 'data_type' in field and field['data_type'] is not None:
                type = field['data_type']
            fields.append(SchemaField(
                name=field['name'], type=type
            ))
        return fields

    def extract_namespace(self, profile: Dict):
        if profile['type'] == 'snowflake':
            self.namespace = f"snowflake://{profile['account']}"
        elif profile['type'] == 'bigquery':
            self.namespace = "bigquery"
        else:
            raise NotImplementedError(
                f"Only 'snowflake' and 'bigquery' adapters are supported right now. "
                f"Passed {profile['type']}"
            )
