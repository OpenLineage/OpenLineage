import prefect

from openlineage.prefect.adapter import OpenLineageAdapter
from openlineage.prefect.tasks.external_dataset import ExternalDataset
from openlineage.prefect.test_utils import RESOURCES
from openlineage.prefect.test_utils.tasks import test_flow


class TestAdapter:
    def setup(self):
        self.adapter = OpenLineageAdapter()

    def test_create_source(self):
        created = self.adapter.create_source(
            source_name="my-source",
            source_type="POSTGRESQL",
            connection_url="jdbc:postgresql://db.example.com/mydb",
            description="My first source!"
        )
        assert created

        created = self.adapter.create_source(
            **{'source_name': 'local', 'source_type': 'FILE', 'connection_url': "file:file://"}
        )
        assert created

    def test_list_sources(self):
        r = self.adapter.list_sources()
        assert r

    def test_create_dataset(self):
        created = self.adapter.create_dataset(
            **{
                "type": "DB_TABLE",
                "url_path": "public.mytable",
                "namespace": "default",
                "dataset_name": "my-dataset",
                "source_name": "my-source",
                "fields": [
                    {
                        "name": "a",
                        "type": "INTEGER"
                    },
                    {
                        "name": "b",
                        "type": "TIMESTAMP"
                    },
                    {
                        "name": "c",
                        "type": "INTEGER"
                    },
                    {
                        "name": "d",
                        "type": "INTEGER"
                    }
                ],
                "description": "My first dataset!"
            }
        )
        assert created
