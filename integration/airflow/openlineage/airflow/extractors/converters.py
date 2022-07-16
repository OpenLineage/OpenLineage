from openlineage.client.run import Dataset


def table_to_dataset(table: Table):
    from airflow.lineage.entities import Table

    return Dataset(
        namespace=f"{table.cluster}",
        name=f"{table.database}.{table.name}",
        facets={
            "tags": table.tags,
            "description": table.description,
            "columns": table.columns,
            "owners": table.owners,
            "extra": table.extra,
        }
    )
