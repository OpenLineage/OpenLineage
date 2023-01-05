# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

def test_table_to_dataset_conversion():
    from openlineage.airflow.extractors.converters import convert_to_dataset
    from airflow.lineage.entities import Table
    t = Table(
        database="db",
        cluster="c",
        name="table1",
    )

    d = convert_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"


def test_dataset_to_dataset_conversion():
    from openlineage.airflow.extractors.converters import convert_to_dataset
    from openlineage.client.run import Dataset
    t = Dataset(
        namespace="c",
        name="db.table1",
        facets={},
    )

    d = convert_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
