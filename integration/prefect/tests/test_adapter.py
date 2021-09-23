import prefect

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.tasks.external_dataset import ExternalDataset
from openlineage.prefect.test_utils import RESOURCES
from openlineage.prefect.test_utils.tasks import test_flow


class TestAdapter:
    def setup(self):
        self.adapter = OpenLineageAdapter()
