import os
import logging
from marquez_client import Clients
from marquez_airflow.extractors import (Dataset, Source)
from marquez_client.models import JobType
from airflow.utils.state import State

_DAG_DEFAULT_OWNER = 'anonymous'
_DAG_DEFAULT_NAMESPACE = 'default'

_DAG_NAMESPACE = os.getenv(
    'MARQUEZ_NAMESPACE', _DAG_DEFAULT_NAMESPACE
)

log = logging.getLogger(__name__)


class Marquez:
    _client = None

    def get_or_create_marquez_client(self):
        if not self._client:
            self._client = Clients.new_write_only_client()
        return self._client

    def create_namespace(self):
        # TODO: Use 'anonymous' owner for now, but we may want to use
        # the 'owner' attribute defined via default_args for a DAG
        log.debug(
            f"Creating namespace '{_DAG_NAMESPACE}' with "
            f"owner '{_DAG_DEFAULT_OWNER}'...")
        self.get_or_create_marquez_client() \
            .create_namespace(_DAG_NAMESPACE, _DAG_DEFAULT_OWNER)

    def create_run(self, run_id, step, run_args, start_time, end_time) -> str:
        marquez_client = self.get_or_create_marquez_client()
        marquez_client.create_job_run(
            namespace_name=_DAG_NAMESPACE,
            job_name=step.name,
            run_id=run_id,
            run_args=run_args,
            nominal_start_time=start_time,
            nominal_end_time=end_time)
        return run_id

    def create_datasets(self, datasets, marquez_job_run_id=None):
        client = self.get_or_create_marquez_client()
        for dataset in datasets:
            if isinstance(dataset, Dataset):
                self.create_source(dataset.source)
                # NOTE: The client expects a dict when capturing
                # fields for a dataset. Below we translate a field
                # object into a dict for compatibility. Work is currently
                # in progress to make this step unnecessary (see:
                # https://github.com/MarquezProject/marquez-python/pull/89)
                fields = []
                for field in dataset.fields:
                    fields.append({
                        'name': field.name,
                        'type': field.type,
                        'tags': field.tags,
                        'description': field.description
                    })
                client.create_dataset(
                    dataset_name=dataset.name,
                    dataset_type=dataset.type,
                    physical_name=dataset.name,
                    source_name=dataset.source.name,
                    namespace_name=_DAG_NAMESPACE,
                    fields=fields,
                    run_id=marquez_job_run_id)

    def create_source(self, source):
        if isinstance(source, Source):
            client = self.get_or_create_marquez_client()
            client.create_source(
                source.name,
                source.type,
                source.connection_url)

    def create_job(self, step, location, description, state=None, run_id=None):
        # Create a job to ensure it exists
        self.get_or_create_marquez_client().create_job(
            namespace_name=_DAG_NAMESPACE,
            job_name=step.name,
            job_type=JobType.BATCH,
            location=step.location or location,
            input_dataset=self._register_dataset(step.inputs),
            output_dataset=self._register_dataset(step.outputs, run_id)
            if state in {State.SUCCESS, State.SKIPPED}
            else self._register_dataset(step.outputs),
            context=step.context or {},
            description=description,
            run_id=run_id)

    def _register_dataset(self, step_dataset, run_id=None):
        inputs = None
        if step_dataset:
            self.create_datasets(step_dataset, run_id)
            inputs = self._to_dataset_ids(step_dataset)
        return inputs

    def complete_run(self, run_id, at):
        self.get_or_create_marquez_client(). \
            mark_job_run_as_completed(run_id=run_id, at=at)

    def fail_run(self, run_id, at):
        self.get_or_create_marquez_client().mark_job_run_as_failed(
            run_id=run_id, at=at)

    def start_run(self, run_id, start):
        self.get_or_create_marquez_client() \
            .mark_job_run_as_started(run_id, start)

    def _to_dataset_ids(self, datasets):
        if not datasets:
            return None

        return list(map(lambda ds: {
            'namespace': _DAG_NAMESPACE,
            'name': ds.name
        }, datasets))
