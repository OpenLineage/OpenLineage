/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.lineage.TableLineageDataset;
import org.apache.flink.table.planner.lineage.TableLineageDatasetImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcTableLineageDatasetIdentifierVisitorTest {
  JdbcTableLineageDatasetIdentifierVisitor visitor = new JdbcTableLineageDatasetIdentifierVisitor();

  ContextResolvedTable contextResolvedTable = mock(ContextResolvedTable.class, RETURNS_DEEP_STUBS);
  CatalogBaseTable catalogBaseTable = mock(CatalogBaseTable.class);
  TableLineageDataset table;

  @BeforeEach
  void setup() {
    when(contextResolvedTable.getTable()).thenReturn(catalogBaseTable);
    when(contextResolvedTable.getIdentifier().asSummaryString()).thenReturn("tableName");

    table =
        new TableLineageDatasetImpl(
            contextResolvedTable,
            Optional.of(
                new LineageDataset() {
                  @Override
                  public String name() {
                    return "tableName";
                  }

                  @Override
                  public String namespace() {
                    return "";
                  }

                  @Override
                  public Map<String, LineageDatasetFacet> facets() {
                    return Map.of();
                  }
                }));
  }

  @Test
  void testIsDefinedAt() {
    assertThat(visitor.isDefinedAt(mock(LineageDataset.class))).isFalse();
    assertThat(visitor.isDefinedAt(table)).isFalse();

    when(table.table().getOptions()).thenReturn(Map.of("connector", "jdbc"));
    assertThat(visitor.isDefinedAt(table)).isTrue();
  }

  @Test
  void testApply() {
    when(catalogBaseTable.getOptions())
        .thenReturn(
            Map.of(
                "connector",
                "jbcc",
                "url",
                "jdbc:trino://trino:8080",
                "table-name",
                "catalog.schema.some-table"));

    Collection<DatasetIdentifier> datasetIdentifiers = visitor.apply(table);

    assertThat(datasetIdentifiers.size()).isEqualTo(1);
    assertThat(datasetIdentifiers.iterator().next())
        .hasFieldOrPropertyWithValue("name", "catalog.schema.some-table")
        .hasFieldOrPropertyWithValue("namespace", "trino://trino:8080");
  }
}
