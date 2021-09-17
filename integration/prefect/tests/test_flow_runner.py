import os

import prefect
from prefect import task, Flow

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.flow_runner import OpenLineageFlowRunner
from openlineage.prefect.test_utils.tasks import test_flow


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self.runner_cls = OpenLineageFlowRunner

    def test_flow_run(self):
        self.flow.run(p=1, runner_cls=self.runner_cls)

    def test_task_gets_lineage_context(self):
        @task()
        def test():
            lineage: OpenLineageAdapter = prefect.context.lineage
            return lineage.ping()

        with Flow("test") as flow:
            test()

        flow.run(runner_cls=self.runner_cls)

    def test_full_lineage_example(self):
        self.flow.run(runner_cls=self.runner_cls)

