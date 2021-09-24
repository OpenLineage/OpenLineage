import os
from unittest.mock import patch

import prefect
from openlineage.client import OpenLineageClient
from prefect import task, Flow
from requests import Response

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.flow_runner import OpenLineageFlowRunner
from openlineage.prefect.test_utils.tasks import test_flow


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self.runner_cls = OpenLineageFlowRunner

    @patch.object(OpenLineageClient, "emit")
    @patch.object(OpenLineageAdapter, "ping", return_value=True)
    def test_flow_run(self, _, mock_emit):
        self.flow.run(p=1, runner_cls=self.runner_cls)

    @patch.object(OpenLineageAdapter, "ping", return_value=True)
    def test_task_gets_lineage_context(self, _):
        @task()
        def test():
            lineage: OpenLineageAdapter = prefect.context.lineage
            return lineage.ping()

        with Flow("test") as flow:
            test()

        flow.run(runner_cls=self.runner_cls)

    @patch("openlineage.prefect.adapter.OpenLineageClient")
    def test_full_lineage_example(self, mock_open_lineage_client):
        mock_open_lineage_client.session.get = Response.ok
        self.flow.run(p=1, runner_cls=self.runner_cls)
