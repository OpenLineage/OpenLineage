/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.source.RewriteTableStub;
import org.apache.iceberg.spark.source.SparkTable;
import org.junit.jupiter.api.Test;

class IcebergSparkTablesTest {

  private final Table icebergTable = mock(Table.class);

  @Test
  void testSparkTableIsUnwrapped() {
    SparkTable sparkTable = mock(SparkTable.class);
    when(sparkTable.table()).thenReturn(icebergTable);

    assertThat(IcebergSparkTables.fromIcebergSparkTable(sparkTable)).containsSame(icebergTable);
  }

  /**
   * The accessor of a wrapper like {@code SparkRewriteTable} is declared on a package-private base
   * class, and must still be reachable from outside Iceberg's package.
   */
  @Test
  void testIcebergWrapperIsUnwrapped() {
    assertThat(IcebergSparkTables.fromIcebergSparkTable(new RewriteTableStub(icebergTable)))
        .containsSame(icebergTable);
  }

  @Test
  void testIcebergWrapperWithoutIcebergTableIsNotUnwrapped() {
    assertThat(IcebergSparkTables.fromIcebergSparkTable(new RewriteTableStub(null))).isEmpty();
  }

  @Test
  void testForeignTableIsNotUnwrapped() {
    assertThat(IcebergSparkTables.fromIcebergSparkTable(new ForeignTableStub(icebergTable)))
        .isEmpty();
  }

  @Test
  void testTableWithoutAccessorIsNotUnwrapped() {
    org.apache.spark.sql.connector.catalog.Table table =
        mock(org.apache.spark.sql.connector.catalog.Table.class);

    assertThat(IcebergSparkTables.fromIcebergSparkTable(table)).isEmpty();
    assertThat(IcebergSparkTables.fromTableAccessor(table)).isEmpty();
  }

  @Test
  void testNullTableIsNotUnwrapped() {
    assertThat(IcebergSparkTables.fromIcebergSparkTable(null)).isEmpty();
    assertThat(IcebergSparkTables.fromTableAccessor(null)).isEmpty();
  }

  /**
   * Tables loaded from an Iceberg {@code SparkCatalog} are known to be Iceberg's, so the catalog
   * path unwraps any table exposing the accessor, whatever its package.
   */
  @Test
  void testTableAccessorIsUsedRegardlessOfPackage() {
    assertThat(IcebergSparkTables.fromTableAccessor(new ForeignTableStub(icebergTable)))
        .containsSame(icebergTable);
    assertThat(IcebergSparkTables.fromTableAccessor(new RewriteTableStub(icebergTable)))
        .containsSame(icebergTable);
  }
}
