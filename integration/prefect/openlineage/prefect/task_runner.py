from typing import Dict

import prefect
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.state import State

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.util import task_qualified_name


class OpenLineageTaskRunner(TaskRunner):
    def __init__(self, *args, lineage_adapter: OpenLineageAdapter, **kwargs):
        super().__init__(*args, **kwargs)
        self._adapter = lineage_adapter
        self.state_handlers.append(self.on_state_changed)
        self.task_inputs = None
        self.inputs_to_tasks = {}

    def on_state_changed(self, _, old_state: State, new_state: State):
        self._adapter.on_state_update(
            task=self.task,
            old_state=old_state,
            new_state=new_state,
            task_inputs=self.task_inputs,
            inputs_to_tasks=self.inputs_to_tasks,
        )

    def get_task_inputs(self, state: State, upstream_states: Dict[Edge, State]) -> Dict[str, Result]:
        for upstream in upstream_states:
            self.inputs_to_tasks[upstream.key] = task_qualified_name(task=upstream.upstream_task)
        task_inputs = super().get_task_inputs(state=state, upstream_states=upstream_states)
        return task_inputs

    def set_task_to_running(self, state: State, inputs: Dict[str, Result]) -> State:
        self.task_inputs = inputs
        state = super().set_task_to_running(state=state, inputs=inputs)
        return state

    def get_task_run_state(self, state: State, inputs: Dict[str, Result]) -> State:
        """Inject the OpenLineageAdapter into the context for the task run"""
        with prefect.context(lineage=self._adapter):
            return super().get_task_run_state(state=state, inputs=inputs)
