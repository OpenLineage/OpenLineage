/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DbTableMetaTest {

  @Test
  void equalTablesHaveEqualHashCodes() {
    // both describe the table "db.table": one parsed from a qualified "db.table" reference,
    // the other from a bare "table" reference resolved against a default database
    DbTableMeta fromQualifiedName = new DbTableMeta(null, "db", "table");
    DbTableMeta fromDefaultDatabase = new DbTableMeta("db", null, "table");

    assertEquals(fromQualifiedName, fromDefaultDatabase);
    assertEquals(fromQualifiedName.hashCode(), fromDefaultDatabase.hashCode());
  }

  @Test
  void equalTablesAreTheSameElementOfASet() {
    Set<DbTableMeta> tables = new HashSet<>();
    tables.add(new DbTableMeta(null, "db", "table"));
    tables.add(new DbTableMeta("db", null, "table"));

    assertEquals(1, tables.size());
  }

  @Test
  void columnsOfEqualTablesResolveToTheSameMapKey() {
    Map<ColumnMeta, String> lineage = new HashMap<>();
    lineage.put(new ColumnMeta(new DbTableMeta(null, "db", "table"), "column"), "expression");

    assertEquals(
        "expression", lineage.get(new ColumnMeta(new DbTableMeta("db", null, "table"), "column")));
  }
}
