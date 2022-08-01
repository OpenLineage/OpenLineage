# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

from openlineage.common.sql import DbTableMeta
from openlineage.airflow.extractors.sql_extractor import SqlExtractor


def test_get_tables_hierarchy():
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Table1"), DbTableMeta("Table2")]
    ) == {None: {None: ["Table1", "Table2"]}}

    # base check with db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")]
    ) == {None: {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # same, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1"], "schema2": ["Table2"]}}

    # explicit db, no cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        database="Db",
    ) == {None: {"schema1": ["Table1", "Table2"]}}

    # explicit db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table1", "Table2"]}}

    # mixed db, with cross db
    assert SqlExtractor._get_tables_hierarchy(
        [DbTableMeta("Db2.Schema1.Table1"), DbTableMeta("Schema1.Table2")],
        database="Db",
        is_cross_db=True,
    ) == {"db": {"schema1": ["Table2"]}, "db2": {"schema1": ["Table1"]}}
