/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package com.clickhouse.spark;

import java.util.Map;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * Minimal stand-in for the connector's {@code com.clickhouse.spark.ClickHouseCatalog}, which is
 * deliberately not a test dependency of this project. The fully-qualified name must stay identical
 * to the real catalog, because {@code ClickHouseHandler#isClass} matches on the class name. Should
 * the real connector ever be added to the test classpath, this stub would shadow it and must be
 * removed.
 */
public class ClickHouseCatalog implements TableCatalog {
  private String catalogName;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table loadTable(Identifier identifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table createTable(
      Identifier identifier,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Table alterTable(Identifier identifier, TableChange... changes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropTable(Identifier identifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renameTable(Identifier oldIdentifier, Identifier newIdentifier) {
    throw new UnsupportedOperationException();
  }
}
