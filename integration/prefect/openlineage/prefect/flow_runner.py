from functools import partial

from prefect.engine import FlowRunner

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.task_runner import OpenLineageTaskRunner


class OpenLineageFlowRunner(FlowRunner):
    def __init__(self, *args, **kwargs):
        self._adapter = OpenLineageAdapter()
        task_runner_cls = partial(OpenLineageTaskRunner, lineage_adapter=self._adapter)
        super().__init__(*args, task_runner_cls=task_runner_cls, **kwargs)

        # Ensure we can connect early - don't want this to trigger tasks to fail inside the flow
        self._adapter.ping()
