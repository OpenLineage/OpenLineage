from unittest.mock import patch, MagicMock

import pendulum
import prefect
from openlineage.client import OpenLineageClient
from openlineage.client.facet import ParentRunFacet, DocumentationJobFacet, DataSourceDatasetFacet
from openlineage.client.run import RunEvent, RunState, Run, Job, OutputDataset
from prefect import Flow, Task
from prefect.engine import FlowRunner
from prefect.engine.state import Success, State, Pending, Running
from prefect.tasks.shell import ShellTask

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.test_utils import RESOURCES
from openlineage.prefect.test_utils.tasks import test_flow


class SuccessTask(Task):
    """A simple task"""

    def run(self):
        return 1


class ErrorTask(Task):
    def run(self):
        raise ValueError("custom-error-message")


class TestAdapter:
    def setup(self):
        self.adapter = OpenLineageAdapter()
        self.task = SuccessTask()
        prefect.context.update(
            **dict(
                flow_name="test-flow",
                task_name="test-task",
                task_run_id="5c6bf446-627b-425d-8cd7-8db027998f42",
                flow_run_id="40991413-2cbe-4fd1-92b0-1e9790bbe104",
                date=pendulum.DateTime(2021, 1, 1),
            )
        )

    def _run_task(self, task_cls: type) -> State:
        flow = Flow(name="flow-" + str(task_cls))
        flow.add_task(task_cls())
        flow_runner = FlowRunner(flow=flow)
        state = flow_runner.run()
        return state

    @patch.object(OpenLineageClient, "emit")
    def test_task_started_to_run_event(self, mock_emit):
        self.adapter.on_state_update(
            old_state=Pending(),
            new_state=Running(),
            task=self.task,
        )
        run_event = mock_emit.call_args.args[0]
        expected = RunEvent(
            eventType=RunState.START,
            eventTime="2021-01-01T00:00:00",
            run=Run(
                runId="5c6bf446-627b-425d-8cd7-8db027998f42",
                facets={
                    "parentRun": ParentRunFacet(
                        run={"runId": "40991413-2cbe-4fd1-92b0-1e9790bbe104"},
                        job={"namespace": "default", "name": "test-flow.test-task"},
                    )
                },
            ),
            job=Job(
                namespace="default",
                name="test-flow.test-task",
                facets={
                    "documentation": DocumentationJobFacet(
                        description="A simple task",
                    )
                },
            ),
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.1.0/integration/prefect",
            inputs=None,
            outputs=None,
        )
        assert run_event == expected

    @patch("openlineage.prefect.adapter.result_location", return_value="2021/1/1/fc357b2e.prefect_result")
    @patch.object(OpenLineageClient, "emit")
    def test_task_success_to_run_event(self, mock_emit, _):
        success = self._run_task(task_cls=SuccessTask)
        self.adapter.on_state_update(old_state=Running(), new_state=success, task=self.task, task_inputs={})
        run_event = mock_emit.call_args.args[0]
        expected = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime="2021-01-01T00:00:00",
            run=Run(runId="5c6bf446-627b-425d-8cd7-8db027998f42", facets={}),
            job=Job(namespace="default", name="test-flow.test-task", facets={}),
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.1.0/integration/prefect",
            inputs=None,
            outputs=OutputDataset(
                namespace="test-flow",
                name="test_adapter.SuccessTask",
                facets={},
                outputFacets={
                    "output-dataset": DataSourceDatasetFacet(
                        name="test_adapter.SuccessTask-output",
                        uri="2021/1/1/fc357b2e.prefect_result",
                    )
                },
            ),
        )
        assert run_event == expected

    @patch.object(OpenLineageClient, "emit")
    def test_task_failed_to_run_event(self, mock_emit):
        failed = self._run_task(task_cls=ErrorTask)

        self.adapter.on_state_update(
            old_state=Running(),
            new_state=failed,
            task=self.task,
        )
        run_event = mock_emit.call_args.args[0]
        expected = RunEvent(
            eventType=RunState.FAIL,
            eventTime="2021-01-01T00:00:00",
            run=Run(
                runId="5c6bf446-627b-425d-8cd7-8db027998f42",
                facets={},
            ),
            job=Job(
                namespace="default",
                name="test-flow.test-task",
            ),
            producer="https://github.com/OpenLineage/OpenLineage/tree/0.1.0/integration/prefect",
            inputs=None,
            outputs=None,
        )
        assert run_event == expected
