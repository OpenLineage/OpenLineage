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


def processor_for(profile, project_dir="/proj"):
    processor = Processor(producer="producer", job_namespace="dbt")
    processor.project_dir = project_dir
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
            ("sqlite:////proj/catalog.db", "default.s.t"),
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
            ("sqlite:////proj/catalog.db", "local.s.t"),
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
        "x": [("sqlite:////proj/catalog.db", "local.s.b")],
    }


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("sqlite:///catalog.db", "sqlite:////proj/catalog.db"),
        (
            "postgresql+psycopg2://u:p@/iceberg?host=db.example.com&port=5433",
            "postgresql://db.example.com:5433/iceberg",
        ),
        ("postgresql:///iceberg?host=::1&port=5433", "postgresql://[::1]:5433/iceberg"),
        ("postgresql:///iceberg?host=[fd00::7]", "postgresql://[fd00::7]/iceberg"),
        # SQLAlchemy reads "#" as part of the database, not as a fragment
        ("sqlite:///catalog#one.db", "sqlite:////proj/catalog#one.db"),
        ("sqlite+pysqlite:////abs/catalog.db", "sqlite:////abs/catalog.db"),
        # the server and its port can be split between the URL and the query
        ("postgresql://db.example.com/iceberg?port=5433", "postgresql://db.example.com:5433/iceberg"),
        ("postgresql://u:p@:5433/iceberg?host=DB.example.com", "postgresql://db.example.com:5433/iceberg"),
        # a default port is the same server as no port
        ("postgresql://db.example.com:5432/iceberg", "postgresql://db.example.com/iceberg"),
        ("postgresql:///iceberg?host=db.example.com&port=5432", "postgresql://db.example.com/iceberg"),
        ("mysql+pymysql://u:p@DB.example.com:3306/iceberg", "mysql://db.example.com/iceberg"),
        ("postgresql://db1.example.com/iceberg?host=db2.example.com", None),
        ("postgresql://db1.example.com/iceberg?host=db1.example.com", None),
        ("postgresql://db.example.com:5433/iceberg?port=5434", None),
        ("postgresql://db.example.com/iceberg?host=/var/run/postgresql", None),
        ("postgresql://db.example.com:bad/iceberg", None),
        ("postgresql://db.example.com:/iceberg", None),
        ("postgresql:///iceberg?host=db&port=70000", None),
        ("iceberg?host=db.example.com", None),
        ("postgresql:///iceberg?host=[]", None),
        ("postgresql:///iceberg?host=u:p@db.example.com", None),
        ("postgresql:///iceberg?host=db,db2", None),
        ("sqlite://host/catalog.db", None),
        # the database is what the driver connects to: unquoted, dbname wins, and is required
        ("postgresql://db.example.com/my%2Ddb", "postgresql://db.example.com/my-db"),
        ("sqlite:///my%20catalog.db", "sqlite:////proj/my catalog.db"),
        ("postgresql://db.example.com/a?dbname=b", "postgresql://db.example.com/b"),
        ("postgresql://db.example.com?dbname=b", "postgresql://db.example.com/b"),
        ("postgresql://alice@db.example.com", None),
        ("postgresql://db.example.com/a?dbname=b&dbname=c", None),
        # one IP address, one spelling
        ("postgresql://[0:0:0:0:0:0:0:1]/iceberg", "postgresql://[::1]/iceberg"),
        ("postgresql:///iceberg?host=0::1", "postgresql://[::1]/iceberg"),
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
    processor = processor_for({"type": "polars", "catalogs": [{**SQL, "pyiceberg_type": "sql", "uri": uri}]})
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
        ("https://[0::1]:8181/api", None, ("https://[::1]:8181/api", "s.t")),
        # Azure storage URLs keep the container, and only it, from the user info part
        (
            "https://c.example.com/api",
            "abfss://prod@acct.dfs.core.windows.net/wh/",
            ("https://c.example.com/api", "abfss://prod@acct.dfs.core.windows.net/wh.s.t"),
        ),
        (
            "https://c.example.com/api",
            "wasbs://container:key@acct.blob.core.windows.net/wh",
            ("https://c.example.com/api", "wasbs://acct.blob.core.windows.net/wh.s.t"),
        ),
        (
            "https://c.example.com/api",
            "s3://user@bucket/wh",
            ("https://c.example.com/api", "s3://bucket/wh.s.t"),
        ),
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


@pytest.mark.parametrize("sql", ['select id from "local"."s"."a"', "select id from s.a"])
def test_column_lineage_is_left_out_when_inputs_in_several_catalogs_share_a_name(sql):
    """Two inputs dbt renders as local.s.a live in different catalogs: no catalog is guessed."""
    processor = processor_for({"type": "polars", "catalog": "local", "catalogs": [REST, SQL]})
    in_lake = model("local", alias="a")
    in_lake.metadata_node["config"] = {"catalog": "lake"}
    in_local = model("local", alias="a")
    assert processor.get_column_lineage("ignored", sql, [in_lake, in_local]) is None
    # with one of them only, the name resolves to it
    (field,) = processor.get_column_lineage("ignored", sql, [in_local]).fields["id"].inputFields
    assert (field.namespace, field.name) == ("sqlite:////proj/catalog.db", "local.s.a")


def test_unused_catalogs_are_never_read():
    unused = LazyJinjaLoadDict(
        {"name": "other", "type": "iceberg", "uri": "{{ env_var('UNSET_CATALOG_URI') }}"},
        jinja_env=Environment(),
    )
    # Rendering the uri would fail: env_var is not defined in this environment.
    processor = processor_for({"type": "polars", "catalog": "local", "catalogs": [SQL, unused]})
    assert processor.dataset_namespace == "sqlite:////proj/catalog.db"
    assert processor.node_to_dataset(model("local")).name == "local.s.t"
    with pytest.raises(Exception, match="env_var"):
        processor.node_to_dataset(model("other"))


@pytest.mark.parametrize(
    "uri",
    [
        "sqlite:///catalog.db",
        "sqlite:///./catalog.db",
        "sqlite:///sub/../catalog.db",
        "sqlite:////proj/catalog.db",
        "sqlite:////proj//catalog.db",
    ],
)
def test_sqlite_catalog_is_identified_by_its_file(uri):
    """dbt-polars opens a relative path from the project directory: every spelling is the same file."""
    processor = processor_for({"type": "polars", "catalogs": [{**SQL, "uri": uri}]})
    assert processor.dataset_namespace == "sqlite:////proj/catalog.db"


def test_sqlite_catalog_follows_symlinks(tmp_path):
    (tmp_path / "real").mkdir()
    (tmp_path / "project").symlink_to(tmp_path / "real")
    processor = processor_for({"type": "polars", "catalogs": [SQL]}, project_dir=str(tmp_path / "project"))
    assert processor.dataset_namespace == f"sqlite:///{(tmp_path / 'real').resolve().as_posix()}/catalog.db"


def test_relative_sqlite_catalog_without_project_dir_is_unsupported():
    processor = processor_for({"type": "polars", "catalogs": [SQL]}, project_dir=None)
    with pytest.raises(NotImplementedError, match="project directory"):
        processor.node_to_dataset(model("local"))


@pytest.mark.parametrize(
    ("prefix", "expected"),
    [
        (None, "https://c.example.com/api"),
        ("", "https://c.example.com/api"),
        ("a", "https://c.example.com/api/v1/a"),
        ("/a/", "https://c.example.com/api/v1/a"),
        ("b", "https://c.example.com/api/v1/b"),
    ],
)
def test_rest_catalog_prefix_selects_the_endpoint(prefix, expected):
    """pyiceberg calls {uri}/v1/{prefix}/...: catalogs that differ by prefix are different catalogs."""
    entry = {**REST, "uri": "https://c.example.com/api/", "warehouse": None, "prefix": prefix}
    processor = processor_for({"type": "polars", "catalogs": [entry]})
    assert processor._dataset_identity("lake", "s", "t") == (expected, "s.t")
