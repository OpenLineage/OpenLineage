from openlineage.client.run import Dataset
from airflow.lineage.entities import Table


def table_to_dataset(table: Table):
    return Dataset(
        namespace=f"{table.cluster}",
        name=f"{table.database}.{table.name}",
        facets={},
    )
