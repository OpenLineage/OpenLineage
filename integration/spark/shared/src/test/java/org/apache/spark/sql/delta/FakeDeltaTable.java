/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.spark.sql.delta;

import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;

public class FakeDeltaTable implements Table {

  @Override
  public String name() {
    return "fake-delta-table";
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
