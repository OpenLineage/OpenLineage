import prefect

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.tasks.external_dataset import ExternalDataset
from openlineage.prefect.test_utils import RESOURCES
from openlineage.prefect.test_utils.tasks import test_flow


class TestExternalDataset:
    def setup(self):
        self.flow = test_flow
        self.adapter = OpenLineageAdapter()

    def test_external_dataset_lineage(self):
        with prefect.context(lineage=self.adapter):
            t = ExternalDataset(
                source_name="local",
                source_type="FILE",
                source_connection_url="file://",
                dataset_url_path=str(RESOURCES.joinpath("1.json")),
                dataset_name='json'
            )
            result = t.run()
            assert result == b"1"
