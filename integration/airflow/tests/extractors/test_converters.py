# Copyright 2018-2023 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
from openlineage.airflow.extractors.converters import convert_to_dataset
from openlineage.client.run import Dataset

from openlineage.client.facet import SchemaDatasetFacet, SchemaField


def test_table_to_dataset_conversion():
    from airflow.lineage.entities import Table

    t = Table(
        database="db",
        cluster="c",
        name="table1",
    )

    d = convert_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
    assert d.facets == {}


def test_table_with_columns_to_dataset_conversion():
    from airflow.lineage.entities import Column, Table

    t = Table(
        database="db",
        cluster="c",
        name="table1",
        columns=[Column(name="col1", description="col1 desc", data_type="int")],
    )

    d = convert_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"
    assert d.facets.get("schema") == SchemaDatasetFacet(
        fields=[
            SchemaField(
                name="col1",
                type="int",
                description="col1 desc",
            )
        ]
    )


def test_dataset_to_dataset_conversion():
    t = Dataset(
        namespace="c",
        name="db.table1",
        facets={},
    )

    d = convert_to_dataset(t)

    assert d.namespace == "c"
    assert d.name == "db.table1"


@pytest.mark.parametrize(
    "uri, dataset",
    [
        ("s3://my-bucket/my-file", Dataset("s3://my-bucket", "/my-file")),
        (
            "gcs://my-bucket/some/path/to/file",
            Dataset("gs://my-bucket", "/some/path/to/file"),
        ),
        (
            "gs://my-bucket/some/path/to/file",
            Dataset("gs://my-bucket", "/some/path/to/file"),
        ),
        ("something://asdf", Dataset("something", "/asdf")),
        ("file://path/to/something", Dataset("file", "/path/to/something")),
        ("not|a|uri", None),
    ],
)
def test_gcs_file_to_dataset_conversion(uri, dataset):
    from airflow.lineage.entities import File

    f = File(url=uri)
    d = convert_to_dataset(f)

    if dataset is None:
        assert d is None
    else:
        assert d.namespace == dataset.namespace
        assert d.name == dataset.name
