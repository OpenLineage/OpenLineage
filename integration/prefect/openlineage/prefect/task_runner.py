import datetime
from typing import Dict, List

import prefect
from openlineage.client.facet import DataSourceDatasetFacet
from openlineage.client.run import InputDataset
from openlineage.client.run import OutputDataset
from prefect import Parameter
from prefect.core import Edge
from prefect.engine import TaskRunner
from prefect.engine.result import Result
from prefect.engine.state import Failed
from prefect.engine.state import Pending
from prefect.engine.state import Running
from prefect.engine.state import State
from prefect.engine.state import Success

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.util import task_qualified_name


def utc_now() -> str:
    """Create an openlineage compatible timestamp string"""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()[:-6] + "Z"


def flow_namespace():
    """Create a namespace from a flow (and optionally project) name"""
    project = prefect.context.get("project_name")
    prefix = f"{project}." if project else ""
    return f"{prefix}{prefect.context.flow_name}"


def result_location(result: Result, **raw_inputs) -> str:
    """Determine results location using the same formatting kwargs prefect does in `task_runner.get_task_run_state`"""
    formatting_kwargs = {
        **prefect.context.get("parameters", {}).copy(),
        **prefect.context,
        **raw_inputs,
    }
    clone = result.copy().format(**formatting_kwargs)
    return clone.location


class OpenLineageTaskRunner(TaskRunner):
    def __init__(self, *args, client: OpenLineageAdapter, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = client
        self.task_full_name = task_qualified_name(task=self.task)
        self.state_handlers.append(self.on_state_changed)
        self.task_inputs = None
        self.inputs_to_tasks = {}

    def on_state_changed(self, _, old_state: State, new_state: State):
        if isinstance(old_state, Running) and isinstance(new_state, Success):
            self._on_success(new_state)
        elif isinstance(old_state, (Pending, Running)) and isinstance(new_state, Failed):
            self._on_failure(state=new_state)

    def _prefect_inputs_to_input_dataset(self, inputs: Dict[str, Result]) -> List[InputDataset]:
        """Convert prefect inputs to input Datasets for OpenLineage"""
        if not inputs:
            return []
        return [
            InputDataset(
                namespace=flow_namespace(),
                name=self.inputs_to_tasks[k],
                facets={},
                inputFacets={},
            )
            for k, v in inputs.items()
        ]

    def prefect_result_to_output_dataset(self, result: Result) -> OutputDataset:
        output_facets = {}
        if not isinstance(self.task, Parameter):
            output_facets["output-dataset"] = DataSourceDatasetFacet(
                name=f"{self.task_full_name}-output",
                uri=result_location(result, **self.task_inputs),
            )
        return OutputDataset(
            namespace=flow_namespace(),
            name=f"{self.task_full_name}",
            facets={},
            outputFacets=output_facets,
        )

    def _task_description(self):
        if isinstance(self.task, Parameter):
            # Parameters don't have any doc / description at this stage, simply return the name
            return self.task.name
        else:
            return self.task.__doc__

    def _on_start(self, inputs: Dict[str, Result]):
        context = prefect.context
        run_id = self._client.start_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            job_description=self._task_description(),
            event_time=utc_now(),
            parent_run_id=context.flow_run_id,
            code_location=None,
            inputs=self._prefect_inputs_to_input_dataset(inputs),
            outputs=None,
        )
        self.logger.debug(f"OpenLineage run CREATED run_id: {run_id}")

    def _on_success(self, state: Success):
        context = prefect.context
        run_id = self._client.complete_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            inputs=None,
            end_time=utc_now(),
            outputs=[self.prefect_result_to_output_dataset(result=state._result)],
        )
        self.logger.info(f"OpenLineage run COMPLETE run_id: {run_id}")

    def _on_failure(self, state: Failed):
        context = prefect.context
        run_id = self._client.fail_task(
            run_id=context.task_run_id,
            job_name=self.task_full_name,
            inputs=None,
            outputs=None,
            end_time=utc_now(),
        )
        self.logger.info(f"OpenLineage run FAILED run_id: {run_id}")

    def get_task_inputs(self, state: State, upstream_states: Dict[Edge, State]) -> Dict[str, Result]:
        for upstream in upstream_states:
            self.inputs_to_tasks[upstream.key] = task_qualified_name(task=upstream.upstream_task)
        task_inputs = super().get_task_inputs(state=state, upstream_states=upstream_states)
        return task_inputs

    def set_task_to_running(self, state: State, inputs: Dict[str, Result]) -> State:
        self.task_inputs = inputs
        state = super().set_task_to_running(state=state, inputs=inputs)
        self._on_start(inputs=inputs)
        return state
