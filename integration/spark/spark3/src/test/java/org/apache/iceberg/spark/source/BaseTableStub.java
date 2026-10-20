/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.iceberg.spark.source;

import org.apache.iceberg.Table;
import org.apache.spark.sql.types.StructType;

/**
 * Mirrors Iceberg's package-private {@code BaseSparkTable}: the {@code table()} accessor is public,
 * but it is declared on a class that code outside this package cannot see.
 */
abstract class BaseTableStub implements org.apache.spark.sql.connector.catalog.Table {
  private final Table icebergTable;

  BaseTableStub(Table table) {
    this.icebergTable = table;
  }

  public Table table() {
    return icebergTable;
  }

  @Override
  public String name() {
    return icebergTable == null ? "stub" : icebergTable.name();
  }

  @Override
  public StructType schema() {
    return new StructType();
  }
}
