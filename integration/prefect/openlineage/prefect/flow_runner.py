from functools import partial

from prefect.engine import FlowRunner
from prefect.engine.state import State

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.task_runner import OpenLineageTaskRunner


class OpenLineageFlowRunner(FlowRunner):
    def __init__(self, *args, **kwargs):
        self._client = OpenLineageAdapter()
        task_runner_cls = partial(OpenLineageTaskRunner, client=self._client)
        super().__init__(*args, task_runner_cls=task_runner_cls, **kwargs)

        # Ensure we're connected early on - don't want this to trigger tasks to fail inside the flow
        self._client.ping()

    def run(self, *args, **kwargs):
        return super().run(*args, **kwargs)

    def get_flow_run_state(self, *args, **kwargs) -> State:
        state = super().get_flow_run_state(*args, **kwargs)
        return state
