/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.Set;
import org.apache.iceberg.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;

/**
 * Stands in for Iceberg's {@code SparkRewriteTable} (Iceberg 1.11+, Spark 4.1), which is not on
 * this module's test classpath: a public table in Iceberg's Spark source package that wraps an
 * Iceberg {@link Table} but, like every {@code BaseSparkTable} subclass other than {@link
 * SparkTable}, is not a {@link SparkTable}.
 */
public class RewriteTableStub extends BaseTableStub {

  public RewriteTableStub(Table table) {
    super(table);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }
}
