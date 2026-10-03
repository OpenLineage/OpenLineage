/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

/** A table outside Iceberg's package that happens to expose an Iceberg table accessor. */
public class ForeignTableStub implements org.apache.spark.sql.connector.catalog.Table {
  private final Table icebergTable;

  ForeignTableStub(Table table) {
    this.icebergTable = table;
  }

  public Table table() {
    return icebergTable;
  }

  @Override
  public String name() {
    return "foreign";
  }

  @Override
  public StructType schema() {
    return new StructType();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }
}
