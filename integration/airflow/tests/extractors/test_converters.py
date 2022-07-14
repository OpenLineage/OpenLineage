from airflow.lineage.entities import Table

from openlineage.airflow.extractors.converters import table_to_dataset

def test_table_to_dataset_conversion():
    t = Table(
        database="db",
        cluster="c",
        name="table1",
        tags="example"
    )

    d = table_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
    assert d.facets["tags"] == "example"