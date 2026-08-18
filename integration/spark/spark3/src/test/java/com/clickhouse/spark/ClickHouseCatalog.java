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
