# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
from openlineage.common.provider.dbt.processor import Adapter, DbtArtifactProcessor, ModelNode

REST = {
    "name": "lake",
    "type": "iceberg",
    "pyiceberg_type": "rest",
    "schema": "analytics",
    "uri": "HTTPS://user:secret@Catalog.Example.com:443/api/catalog/?token=abc",
    "warehouse": "lakehouse",
    "token": "secret-token",
}
SQL = {"name": "local", "type": "iceberg", "schema": "dev", "uri": "sqlite:///catalog.db"}
FILES = {"name": "files", "type": "local", "schema": "raw"}


class Processor(DbtArtifactProcessor):
    def get_dbt_metadata(self): ...

    def dbt_run_run_facet(self):
        return {}


def processor_for(profile):
    processor = Processor(producer="producer", job_namespace="dbt")
    processor.extract_adapter_type(profile)
    processor.extract_dataset_namespace(profile)
    return processor


def model(database):
    return ModelNode(
        type="model", metadata_node={"database": database, "schema": "s", "alias": "t", "columns": {}}
    )


@pytest.mark.parametrize(
    ("profile", "database", "expected"),
    [
        # single-catalog shorthand: the catalog is named "default"
        (
            {"type": "polars", "catalog_type": "iceberg", "schema": "dev", "uri": "sqlite:///catalog.db"},
            "default",
            ("sqlite:///catalog.db", "default.s.t"),
        ),
        # multiple catalogs: each relation uses the catalog in its database
        (
            {"type": "polars", "catalog": "local", "catalogs": [REST, SQL]},
            "lake",
            ("https://catalog.example.com/api/catalog", "lakehouse.s.t"),
        ),
        (
            {"type": "polars", "catalog": "local", "catalogs": [REST, SQL]},
            None,
            ("sqlite:///catalog.db", "local.s.t"),
        ),
    ],
)
def test_dataset_identity(profile, database, expected):
    processor = processor_for(profile)
    assert processor.adapter_type == Adapter.POLARS
    namespace, name, facets, _ = processor.extract_dataset_data(model(database), None, has_facets=True)
    assert (namespace, name) == expected
    assert facets["dataSource"].uri == namespace
    assert "secret" not in repr((namespace, name, facets))


def test_unsupported_catalog_raises_when_used():
    processor = processor_for({"type": "polars", "catalogs": [SQL, FILES]})
    with pytest.raises(NotImplementedError, match="catalog type 'local'"):
        processor.node_to_dataset(model("files"))


def test_column_lineage_uses_catalog_identity():
    processor = processor_for({"type": "polars", "catalogs": [REST, SQL]})
    lineage = processor.get_column_lineage(
        "ignored", 'select a.id, b.x from "lake"."s"."a" a join "local"."s"."b" b on a.id = b.id'
    )
    assert {
        name: [(f.namespace, f.name) for f in field.inputFields] for name, field in lineage.fields.items()
    } == {
        "id": [("https://catalog.example.com/api/catalog", "lakehouse.s.a")],
        "x": [("sqlite:///catalog.db", "local.s.b")],
    }
