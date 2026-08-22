/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkDatasetBuilder;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.Collections;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.delta.sources.DeltaSink;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.streaming.FileStreamSink;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.Option;

class WriteToMicroBatchDataSourceV1DatasetBuilderTest {

  private DatasetFactory<OpenLineage.OutputDataset> factory;
  private WriteToMicroBatchDataSourceV1DatasetBuilder builder;
  private WriteToMicroBatchDataSourceV1 writeToMicroBatchV1;
  private FileStreamSink fileStreamSink;
  private Sink unsupportedSink;
  private SparkListenerSQLExecutionEnd event;
  private StructType schema;

  @BeforeEach
  void setUp() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    SparkSession sparkSession = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);

    OpenLineageContext openLineageContext =
        OpenLineageContext.builder()
            .sparkSession(sparkSession)
            .sparkContext(sparkContext)
            .openLineage(openLineage)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();

    @SuppressWarnings("unchecked")
    DatasetFactory<OpenLineage.OutputDataset> typedFactory = mock(DatasetFactory.class);
    factory = typedFactory;
    builder = new WriteToMicroBatchDataSourceV1DatasetBuilder(openLineageContext, factory);

    writeToMicroBatchV1 = mock(WriteToMicroBatchDataSourceV1.class);
    fileStreamSink = mock(FileStreamSink.class);
    unsupportedSink = mock(Sink.class);

    QueryExecution queryExecution = mock(QueryExecution.class);
    when(queryExecution.analyzed()).thenReturn(writeToMicroBatchV1);

    event = mock(SparkListenerSQLExecutionEnd.class);
    when(event.qe()).thenReturn(queryExecution);

    schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("name", DataTypes.StringType, true)
            });
  }

  @Test
  void testIsDefinedAtLogicalPlan() {
    assertTrue(builder.isDefinedAtLogicalPlan(writeToMicroBatchV1));
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testIsDefinedAtEvent_WithCorrectEvent() {
    assertTrue(builder.isDefinedAt(event));
  }

  @Test
  void testIsDefinedAtEvent_WithIncorrectEvent() {
    SparkListenerEvent wrongEvent = mock(SparkListenerEvent.class);
    assertFalse(builder.isDefinedAt(wrongEvent));
  }

  @Test
  void testIsDefinedAtEvent_WithCorrectEventButWrongLogicalPlan() {
    QueryExecution queryExecution = mock(QueryExecution.class);
    when(queryExecution.analyzed()).thenReturn(mock(LogicalPlan.class));
    when(event.qe()).thenReturn(queryExecution);

    assertFalse(builder.isDefinedAt(event));
  }

  @Test
  void testApply_WithUnknownSinkAndNoCatalogTable() {
    when(writeToMicroBatchV1.sink()).thenReturn(unsupportedSink);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertTrue(result.isEmpty());
    verify(factory, never()).sparkDatasetBuilder();
  }

  @Test
  void testApply_WithFileStreamSink_NoCatalogTable() {
    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);
    assertTrue(result.isEmpty());
  }

  @Test
  void testApply_WithCatalogTableRegardlessOfSink() {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    CatalogTable catalogTable = mock(CatalogTable.class);

    when(writeToMicroBatchV1.sink()).thenReturn(unsupportedSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.apply(catalogTable));

    @SuppressWarnings("unchecked")
    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mock(SparkDatasetBuilder.class);
    when(factory.sparkDatasetBuilder()).thenReturn(sparkBuilder);
    when(sparkBuilder.dataset(catalogTable)).thenReturn(sparkBuilder);
    when(sparkBuilder.schema(schema)).thenReturn(sparkBuilder);
    when(sparkBuilder.build()).thenReturn(expectedDataset);

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertEquals(1, result.size());
    assertEquals(expectedDataset, result.get(0));
    verify(sparkBuilder).dataset(catalogTable);
    verify(sparkBuilder).schema(schema);
  }

  @Test
  void testApply_WithDeltaSinkPathWriteOptionAndNoCatalogTable() {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    DeltaSink deltaSink = mock(DeltaSink.class);

    when(writeToMicroBatchV1.sink()).thenReturn(deltaSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(
                Collections.singletonMap("path", "/tmp/delta_target")));

    @SuppressWarnings("unchecked")
    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mock(SparkDatasetBuilder.class);
    when(factory.sparkDatasetBuilder()).thenReturn(sparkBuilder);
    when(sparkBuilder.dataset(any(DatasetIdentifier.class))).thenReturn(sparkBuilder);
    when(sparkBuilder.schema(schema)).thenReturn(sparkBuilder);
    when(sparkBuilder.build()).thenReturn(expectedDataset);

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertEquals(1, result.size());
    assertEquals(expectedDataset, result.get(0));
    ArgumentCaptor<DatasetIdentifier> identifierCaptor =
        ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(sparkBuilder).dataset(identifierCaptor.capture());
    assertEquals("/tmp/delta_target", identifierCaptor.getValue().getName());
    assertEquals("file", identifierCaptor.getValue().getNamespace());
    verify(sparkBuilder).schema(schema);
  }

  @Test
  void testApply_WithDeltaSinkPathWriteOptionIsCaseInsensitive() {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    DeltaSink deltaSink = mock(DeltaSink.class);

    when(writeToMicroBatchV1.sink()).thenReturn(deltaSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(
                Collections.singletonMap("PaTh", "/tmp/delta_target")));

    @SuppressWarnings("unchecked")
    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mock(SparkDatasetBuilder.class);
    when(factory.sparkDatasetBuilder()).thenReturn(sparkBuilder);
    when(sparkBuilder.dataset(any(DatasetIdentifier.class))).thenReturn(sparkBuilder);
    when(sparkBuilder.schema(schema)).thenReturn(sparkBuilder);
    when(sparkBuilder.build()).thenReturn(expectedDataset);

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertEquals(1, result.size());
    ArgumentCaptor<DatasetIdentifier> identifierCaptor =
        ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(sparkBuilder).dataset(identifierCaptor.capture());
    assertEquals("/tmp/delta_target", identifierCaptor.getValue().getName());
  }

  @Test
  void testApply_WithDeltaSinkAndNoPathWriteOption() {
    DeltaSink deltaSink = mock(DeltaSink.class);

    when(writeToMicroBatchV1.sink()).thenReturn(deltaSink);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions()).thenReturn(ScalaConversionUtils.asScalaMapEmpty());

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertTrue(result.isEmpty());
    verify(factory, never()).sparkDatasetBuilder();
  }
}
