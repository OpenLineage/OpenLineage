# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import mock
from openlineage.airflow.extractors.sql_extractor import SqlExtractor
from openlineage.client.facet_v2 import column_lineage_dataset
from openlineage.common.dataset import Dataset, Source
from openlineage.common.sql import ColumnLineage, ColumnMeta, DbTableMeta, parse


def normalize_name_lower(name: str) -> str:
    return name.lower()


def test_get_tables_hierarchy():
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Table1"), DbTableMeta("Table2")], normalize_name_lower
    ) == {None: {None: ["Table1", "Table2"]}}

    # base check with db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
        normalize_name_lower,
    ) == {None: {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # same, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
        normalize_name_lower,
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # explicit db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
    ) == {None: {"schema1": ["Table1", "Table2"]}}

    # explicit db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1", "Table2"]}}

    # mixed db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db2.Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        normalize_name_lower,
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table2"]}, "db2": {"schema1": ["Table1"]}}

    # cross db, no db & schema parsed
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Table1"), DbTableMeta("Table2")],
        normalize_name_lower,
        database="Db",
        is_cross_db=True,
    ) == {"db": {None: ["Table1", "Table2"]}}


def test_get_sql_iterator():
    assert SqlExtractor._normalize_sql("select * from asdf") == "select * from asdf"

    assert (
        SqlExtractor._normalize_sql(["select * from asdf", "insert into asdf values (1,2,3)"])
        == "select * from asdf;\ninsert into asdf values (1,2,3)"
    )

    assert (
        SqlExtractor._normalize_sql("select * from asdf;insert into asdf values (1,2,3)")
        == "select * from asdf;\ninsert into asdf values (1,2,3)"
    )


def test_sql_parse():
    query = """
    INSERT INTO
        dest_table
    SELECT
        a.column1 + a.column2 as column_sum, EXTRACT(ISODOW FROM b.date) as day
    FROM
        database1.schema1.table1 a
    LEFT JOIN
        schema2.table2 b ON a.id = b.id
    UNION
    SELECT
        column99 + column98 as column_sum, NULL AS day
    FROM table3;
    """
    sql_meta = parse(query)
    assert sql_meta.errors == []
    assert sql_meta.in_tables == [
        DbTableMeta("table3"),
        DbTableMeta("schema2.table2"),
        DbTableMeta("database1.schema1.table1"),
    ]
    assert sql_meta.out_tables == [DbTableMeta("dest_table")]
    assert sql_meta.column_lineage == [
        ColumnLineage(
            descendant=ColumnMeta(name="column_sum", origin=None),
            lineage=[
                ColumnMeta(name="column98", origin=DbTableMeta("table3")),
                ColumnMeta(name="column99", origin=DbTableMeta("table3")),
                ColumnMeta(name="column1", origin=DbTableMeta("database1.schema1.table1")),
                ColumnMeta(name="column2", origin=DbTableMeta("database1.schema1.table1")),
            ],
        ),
        ColumnLineage(
            descendant=ColumnMeta(name="day", origin=None),
            lineage=[
                ColumnMeta(name="date", origin=DbTableMeta("schema2.table2")),
            ],
        ),
    ]


def test_attach_column_facet_empty():
    class _ExampleSqlExtractor(SqlExtractor):
        def _get_database(self):
            return "_get_database_result"

        def _get_scheme(self):
            return "__get_scheme_result"

    mock_meta = mock.MagicMock(column_lineage=[])
    dataset = Dataset(source=Source(), name="name")
    operator = mock.MagicMock()
    _ExampleSqlExtractor(operator).attach_column_facet(
        dataset=dataset, database="database", sql_meta=mock_meta
    )
    assert dataset == Dataset(source=Source(), name="name")


def test_attach_column_facet():
    query = """
    INSERT INTO
        dest_table
    SELECT
        a.column1 + a.column2 as column_sum, EXTRACT(ISODOW FROM b.date) as day
    FROM
        database1.schema1.table1 a
    LEFT JOIN
        schema2.table2 b ON a.id = b.id
    UNION
    SELECT
        column99 + column98 as column_sum, NULL AS day
    FROM table3;
    """
    sql_meta = parse(query)
    dataset = Dataset(source=Source(name="dataset_source_name"), name="name")
    expected_facet = column_lineage_dataset.ColumnLineageDatasetFacet(
        fields={
            "column_sum": column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(
                        namespace="dataset_source_name",
                        name="default_db.default_schema.table3",
                        field="column98",
                    ),
                    column_lineage_dataset.InputField(
                        namespace="dataset_source_name",
                        name="default_db.default_schema.table3",
                        field="column99",
                    ),
                    column_lineage_dataset.InputField(
                        namespace="dataset_source_name",
                        name="database1.schema1.table1",
                        field="column1",
                    ),
                    column_lineage_dataset.InputField(
                        namespace="dataset_source_name",
                        name="database1.schema1.table1",
                        field="column2",
                    ),
                ],
                transformationType="",
                transformationDescription="",
            ),
            "day": column_lineage_dataset.Fields(
                inputFields=[
                    column_lineage_dataset.InputField(
                        namespace="dataset_source_name",
                        name="default_db.schema2.table2",
                        field="date",
                    ),
                ],
                transformationType="",
                transformationDescription="",
            ),
        }
    )

    class _ExampleSqlExtractor(SqlExtractor):
        def _get_database(self):
            return "_get_database_result"

        def _get_scheme(self):
            return "__get_scheme_result"

        @property
        def default_schema(self):
            return "default_schema"

    operator = mock.MagicMock()
    _ExampleSqlExtractor(operator).attach_column_facet(
        dataset=dataset, database="default_db", sql_meta=sql_meta
    )

    assert dataset.custom_facets["columnLineage"].fields == expected_facet.fields
