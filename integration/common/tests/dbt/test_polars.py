# Copyright 2018-2026 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import pytest
from jinja2 import Environment
from openlineage.common.provider.dbt.local import LazyJinjaLoadDict
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


def model(database, alias="t"):
    return ModelNode(
        type="model", metadata_node={"database": database, "schema": "s", "alias": alias, "columns": {}}
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


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("sqlite:///catalog.db", "sqlite:///catalog.db"),
        (
            "postgresql+psycopg2://u:p@/iceberg?host=db.example.com&port=5433",
            "postgresql://db.example.com:5433/iceberg",
        ),
        ("postgresql:///iceberg?host=::1&port=5432", "postgresql://[::1]:5432/iceberg"),
        ("postgresql:///iceberg?host=[fd00::7]", "postgresql://[fd00::7]/iceberg"),
        ("sqlite://", None),
        ("sqlite:///:memory:", None),
        ("sqlite:///file:cat?mode=memory&uri=true", None),
        ("postgresql:///iceberg?host=/var/run/postgresql", None),
        ("postgresql:///iceberg", None),
        ("postgresql://host1:5432,host2:5432/iceberg", None),
        ("postgresql://host1,host2/iceberg", None),
        ("postgresql:///iceberg?host=db&port=5432,5433", None),
        ("postgresql:///iceberg?host=db&port=5432&port=5433", None),
    ],
)
def test_sql_catalog_endpoint(uri, expected):
    processor = processor_for({"type": "polars", "catalogs": [{**SQL, "uri": uri}]})
    if expected:
        assert processor.dataset_namespace == expected
    else:
        with pytest.raises(NotImplementedError):
            processor.node_to_dataset(model("local"))


def test_configured_catalog_wins_over_database():
    processor = processor_for({"type": "polars", "catalog": "local", "catalogs": [REST, SQL]})
    node = model("local")
    node.metadata_node["config"] = {"catalog": "lake"}
    assert processor.node_to_dataset(node).namespace == "https://catalog.example.com/api/catalog"


@pytest.mark.parametrize(
    ("uri", "warehouse", "expected"),
    [
        ("https://c.example.com/api", "s3://bucket/wh/", ("https://c.example.com/api", "s3://bucket/wh.s.t")),
        ("https://c.example.com/api", "lakehouse", ("https://c.example.com/api", "lakehouse.s.t")),
        ("https://c.example.com:bad/api", "lakehouse", None),
    ],
)
def test_rest_catalog_endpoint(uri, warehouse, expected):
    processor = processor_for({"type": "polars", "catalogs": [{**REST, "uri": uri, "warehouse": warehouse}]})
    if expected:
        assert processor._dataset_identity("lake", "s", "t") == expected
    else:
        with pytest.raises(NotImplementedError):
            processor.node_to_dataset(model("lake"))


def test_single_catalog_profile_values_are_rendered_lazily():
    profile = LazyJinjaLoadDict(
        {
            "type": "polars",
            "catalog_type": "iceberg",
            "pyiceberg_type": "rest",
            "schema": "s",
            "uri": "{{ 'https://c.example.com/api' }}",
            "warehouse": "{{ 'lakehouse' }}",
            "token": "{{ unrendered_secret() }}",
        },
        jinja_env=Environment(),
    )
    assert processor_for(profile)._dataset_identity("default", "s", "t") == (
        "https://c.example.com/api",
        "lakehouse.s.t",
    )


def test_column_lineage_follows_the_resolved_inputs():
    """dbt renders a ref with the manifest database, which may not be the configured catalog."""
    processor = processor_for({"type": "polars", "catalog": "local", "catalogs": [REST, SQL]})
    upstream = model("local", alias="a")
    upstream.metadata_node["config"] = {"catalog": "lake"}
    for sql in ('select id from "local"."s"."a"', "select id from s.a"):
        lineage = processor.get_column_lineage("ignored", sql, [upstream])
        (field,) = lineage.fields["id"].inputFields
        assert (field.namespace, field.name) == ("https://catalog.example.com/api/catalog", "lakehouse.s.a")
