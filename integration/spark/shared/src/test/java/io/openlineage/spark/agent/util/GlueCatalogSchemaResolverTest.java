/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for GlueCatalogSchemaResolver */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GlueCatalogSchemaResolverTest {

  @Test
  void testMapColumnsByPosition_SameSize() {
    List<String> rddColumns = Arrays.asList("order_id", "customer_id", "total");
    List<String> catalogColumns = Arrays.asList("orderid", "customerid", "total");

    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).hasSize(3);
    assertThat(mapped.get(0)).isEqualTo("orderid");
    assertThat(mapped.get(1)).isEqualTo("customerid");
    assertThat(mapped.get(2)).isEqualTo("total");
  }

  @Test
  void testMapColumnsByPosition_DifferentSize() {
    List<String> rddColumns = Arrays.asList("order_id", "total");
    List<String> catalogColumns = Arrays.asList("orderid", "customerid", "total");

    // When sizes don't match, should return RDD columns unchanged
    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).hasSize(2);
    assertThat(mapped.get(0)).isEqualTo("order_id");
    assertThat(mapped.get(1)).isEqualTo("total");
  }

  @Test
  void testMapColumnsByPosition_EmptyLists() {
    List<String> rddColumns = Arrays.asList();
    List<String> catalogColumns = Arrays.asList();

    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).isEmpty();
  }

  @Test
  void testMapColumnsByPosition_SingleColumn() {
    List<String> rddColumns = Arrays.asList("order_id");
    List<String> catalogColumns = Arrays.asList("orderid");

    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).hasSize(1);
    assertThat(mapped.get(0)).isEqualTo("orderid");
  }

  @Test
  void testMapColumnsByPosition_CasePreservation() {
    // Verify that catalog column names preserve their original case
    List<String> rddColumns = Arrays.asList("ORDER_ID", "TOTAL");
    List<String> catalogColumns = Arrays.asList("OrderId", "Total");

    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).hasSize(2);
    assertThat(mapped.get(0)).isEqualTo("OrderId");
    assertThat(mapped.get(1)).isEqualTo("Total");
  }

  @Test
  void testMapColumnsByPosition_ReorderedColumns() {
    // When columns are reordered (same names, different positions), should return RDD columns
    List<String> rddColumns = Arrays.asList("total", "orderid");
    List<String> catalogColumns = Arrays.asList("orderid", "total");

    List<String> mapped =
        GlueCatalogSchemaResolver.mapColumnsByPosition(rddColumns, catalogColumns);

    assertThat(mapped).hasSize(2);
    assertThat(mapped.get(0)).isEqualTo("total");
    assertThat(mapped.get(1)).isEqualTo("orderid");
  }
}
