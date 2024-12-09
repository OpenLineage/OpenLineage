/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.Collection;
import java.util.Map;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.table.planner.lineage.TableLineageDataset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

class KafkaTableLineageDatasetIdentifierVisitorTest {
  KafkaTableLineageDatasetIdentifierVisitor visitor =
      new KafkaTableLineageDatasetIdentifierVisitor();

  TableLineageDataset table = mock(TableLineageDataset.class, Answers.RETURNS_DEEP_STUBS);

  @BeforeEach
  void setup() {}

  @Test
  void testIsDefinedAt() {
    assertThat(visitor.isDefinedAt(mock(LineageDataset.class))).isFalse();
    assertThat(visitor.isDefinedAt(table)).isFalse();

    when(table.table().getOptions()).thenReturn(Map.of("connector", "kafka"));
    assertThat(visitor.isDefinedAt(table)).isTrue();
  }

  @Test
  void testApply() {
    when(table.name()).thenReturn("tableName");
    when(table.table().getOptions())
        .thenReturn(
            Map.of(
                "connector",
                "kafka",
                "properties.bootstrap.servers",
                "localhost:1000,localhost:2000",
                "topic",
                "topic-name"));

    Collection<DatasetIdentifier> datasetIdentifiers = visitor.apply(table);

    assertThat(datasetIdentifiers.size()).isEqualTo(1);
    assertThat(datasetIdentifiers.iterator().next())
        .hasFieldOrPropertyWithValue("name", "topic-name")
        .hasFieldOrPropertyWithValue("namespace", "kafka://localhost:1000");

    assertThat(datasetIdentifiers.iterator().next().getSymlinks())
        .containsExactlyInAnyOrder(
            new Symlink("tableName", "kafka://localhost:1000", SymlinkType.TABLE));
  }

  @Test
  void testApplyWithSemicolonBootstraps() {
    when(table.name()).thenReturn("tableName");
    when(table.table().getOptions())
        .thenReturn(
            Map.of(
                "connector",
                "kafka",
                "properties.bootstrap.servers",
                "localhost:1000;localhost:2000",
                "topic",
                "topic-name"));

    Collection<DatasetIdentifier> datasetIdentifiers = visitor.apply(table);
    assertThat(datasetIdentifiers.iterator().next())
        .hasFieldOrPropertyWithValue("namespace", "kafka://localhost:1000");
  }
}
