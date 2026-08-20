/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
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

class KinesisTableLineageDatasetIdentifierVisitorTest {

  private static final String STREAM_ARN = "arn:aws:kinesis:eu-west-1:123456789012:stream/orders";

  KinesisTableLineageDatasetIdentifierVisitor visitor =
      new KinesisTableLineageDatasetIdentifierVisitor();
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

    when(catalogBaseTable.getOptions()).thenReturn(Map.of("connector", "kinesis"));
    assertThat(visitor.isDefinedAt(table)).isFalse();

    when(catalogBaseTable.getOptions())
        .thenReturn(Map.of("connector", "kinesis", "stream.arn", "not-an-arn"));
    assertThat(visitor.isDefinedAt(table)).isFalse();

    when(catalogBaseTable.getOptions())
        .thenReturn(Map.of("connector", "kinesis", "stream.arn", STREAM_ARN));
    assertThat(visitor.isDefinedAt(table)).isTrue();
  }

  @Test
  void testApply() {
    when(catalogBaseTable.getOptions())
        .thenReturn(Map.of("connector", "kinesis", "stream.arn", STREAM_ARN));

    Collection<DatasetIdentifier> identifiers = visitor.apply(table);

    assertThat(identifiers).hasSize(1);
    DatasetIdentifier identifier = identifiers.iterator().next();
    assertThat(identifier.getNamespace()).isEqualTo("arn:aws:kinesis:eu-west-1:123456789012");
    assertThat(identifier.getName()).isEqualTo("stream/orders");
    assertThat(identifier.getSymlinks())
        .containsExactly(
            new Symlink("tableName", "arn:aws:kinesis:eu-west-1:123456789012", SymlinkType.TABLE));
  }

  @Test
  void testApplyDifferentPartition() {
    when(catalogBaseTable.getOptions())
        .thenReturn(
            Map.of(
                "connector",
                "kinesis",
                "stream.arn",
                "arn:aws-cn:kinesis:cn-north-1:999888777666:stream/cn-orders"));

    DatasetIdentifier identifier = visitor.apply(table).iterator().next();
    assertThat(identifier.getNamespace()).isEqualTo("arn:aws-cn:kinesis:cn-north-1:999888777666");
    assertThat(identifier.getName()).isEqualTo("stream/cn-orders");
  }
}
