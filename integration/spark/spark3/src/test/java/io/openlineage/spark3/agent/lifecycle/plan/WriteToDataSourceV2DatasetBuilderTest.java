/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.DataSourceV2RelationDatasetExtractor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2;
import org.apache.spark.sql.execution.streaming.sources.MicroBatchWrite;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.kafka010.KafkaStreamingWrite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class WriteToDataSourceV2DatasetBuilderTest {

  private final SparkOpenLineageExtensionVisitorWrapper extensionVisitor =
      mock(SparkOpenLineageExtensionVisitorWrapper.class);
  private final OpenLineageContext context =
      OpenLineageContext.builder()
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .meterRegistry(new SimpleMeterRegistry())
          .openLineageConfig(new SparkOpenLineageConfig())
          .sparkExtensionVisitorWrapper(extensionVisitor)
          .build();
  private final DatasetFactory<OpenLineage.OutputDataset> factory = mock(DatasetFactory.class);
  private final WriteToDataSourceV2DatasetBuilder builder =
      new WriteToDataSourceV2DatasetBuilder(context);

  private WriteToDataSourceV2 write;
  private MicroBatchWrite microBatchWrite;

  @BeforeEach
  void setUp() {
    write = mock(WriteToDataSourceV2.class);
    microBatchWrite = mock(MicroBatchWrite.class);
    when(write.batchWrite()).thenReturn(microBatchWrite);
    when(microBatchWrite.writeSupport()).thenReturn(mock(StreamingWrite.class));
  }

  @Test
  void isDefinedForNonKafkaMicroBatchWrite() {
    assertThat(builder.isDefinedAtLogicalPlan(write)).isTrue();
  }

  @Test
  void isNotDefinedForKafkaMicroBatchWrite() {
    when(microBatchWrite.writeSupport()).thenReturn(new KafkaStreamingWrite());

    assertThat(builder.isDefinedAtLogicalPlan(write)).isFalse();
  }

  @Test
  void isNotDefinedForBatchWriteOrOtherPlan() {
    when(write.batchWrite()).thenReturn(mock(BatchWrite.class));

    assertThat(builder.isDefinedAtLogicalPlan(write)).isFalse();
    assertThat(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void preservesAllExtensionProvidedOutputs() {
    SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);
    registerRelationBuilder(event);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    Table table = mock(Table.class);
    OpenLineage.OutputDataset firstOutput = mock(OpenLineage.OutputDataset.class);
    OpenLineage.OutputDataset secondOutput = mock(OpenLineage.OutputDataset.class);
    List<OpenLineage.OutputDataset> extensionOutputs = Arrays.asList(firstOutput, secondOutput);

    when(write.relation()).thenReturn(scala.Option.apply(relation));
    when(relation.table()).thenReturn(table);
    when(extensionVisitor.isDefinedAt(table)).thenReturn(true);
    when(extensionVisitor.getOutputs(table, table.getClass().getName()))
        .thenReturn(ImmutablePair.of(extensionOutputs, Collections.emptyList()));

    try (MockedStatic<DataSourceV2RelationDatasetExtractor> extractor =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      List<OpenLineage.OutputDataset> outputs = builder.apply(event, write);

      assertThat(outputs).containsExactly(firstOutput, secondOutput);
      extractor.verifyNoInteractions();
    }
  }

  @Test
  void extractsOrdinaryRelationExactlyOnceThroughDelegation() {
    SparkListenerEvent event = new SparkListenerSQLExecutionEnd(1L, 1L);
    registerRelationBuilder(event);
    DataSourceV2Relation relation = mock(DataSourceV2Relation.class);
    Table table = mock(Table.class);
    OpenLineage.OutputDataset output = mock(OpenLineage.OutputDataset.class);

    when(write.relation()).thenReturn(scala.Option.apply(relation));
    when(relation.table()).thenReturn(table);
    when(extensionVisitor.isDefinedAt(table)).thenReturn(false);

    try (MockedStatic<DataSourceV2RelationDatasetExtractor> extractor =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      extractor
          .when(
              () -> DataSourceV2RelationDatasetExtractor.extract(factory, context, relation, true))
          .thenReturn(Collections.singletonList(output));

      assertThat(builder.apply(event, write)).containsExactly(output);
      extractor.verify(
          () -> DataSourceV2RelationDatasetExtractor.extract(factory, context, relation, true),
          times(1));
    }
  }

  @Test
  void returnsNoOutputForAnEmptyRelation() {
    when(write.relation()).thenReturn(scala.Option.empty());

    try (MockedStatic<DataSourceV2RelationDatasetExtractor> extractor =
        mockStatic(DataSourceV2RelationDatasetExtractor.class)) {
      assertThat(builder.apply(new SparkListenerSQLExecutionEnd(1L, 1L), write)).isEmpty();

      extractor.verifyNoInteractions();
    }
  }

  private void registerRelationBuilder(SparkListenerEvent event) {
    context
        .getOutputDatasetQueryPlanVisitors()
        .add(
            new DataSourceV2RelationOutputDatasetBuilder(context, factory)
                .asQueryPlanVisitor(event));
  }
}
