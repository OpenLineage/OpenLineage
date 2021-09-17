import os

from openlineage.prefect.flow_runner import OpenLineageFlowRunner
from openlineage.prefect.test_utils.tasks import test_flow


class TestCachedFlowRunner:
    def setup(self):
        self.fs_url = os.environ.get("FS_URL", "memory:///")
        self.flow = test_flow
        self.runner_cls = OpenLineageFlowRunner
        # self.fs, self.root = get_fs(self.fs_url)
        # self._clear_fs()

    # def _clear_fs(self):
    #     try:
    #         self.fs.rm(f"{self.root}", recursive=True)
    #     except FileNotFoundError:
    #         pass
    #     self.fs.mkdir(self.root)
    #
    # def _ls(self):
    #     return self.fs.glob(f"{self.root}/**/*")

    def test_flow_run(self):
        self.flow.run(p=1, runner_cls=self.runner_cls)
