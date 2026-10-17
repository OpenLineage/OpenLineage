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
import java.util.Map;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import scala.Option;

class WriteToMicroBatchDataSourceV1DatasetBuilderTest {

  private static final String OUTPUT_PATH = "/tmp/output_target";

  private DatasetFactory<OpenLineage.OutputDataset> factory;
  private WriteToMicroBatchDataSourceV1DatasetBuilder builder;
  private WriteToMicroBatchDataSourceV1 writeToMicroBatchV1;
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

    factory = mockDatasetFactory();
    builder = new WriteToMicroBatchDataSourceV1DatasetBuilder(openLineageContext, factory);

    writeToMicroBatchV1 = mock(WriteToMicroBatchDataSourceV1.class);
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

  @ParameterizedTest
  @ValueSource(classes = {Sink.class, DeltaSink.class, FileStreamSink.class})
  void testApply_WithCatalogTableRegardlessOfSink(Class<? extends Sink> sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    CatalogTable catalogTable = mock(CatalogTable.class);

    when(writeToMicroBatchV1.sink()).thenReturn(mock(sinkType));
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.apply(catalogTable));

    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mockSparkDatasetBuilder();
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

  @ParameterizedTest
  @ValueSource(classes = {DeltaSink.class, FileStreamSink.class})
  void testApply_WithPathSinkPathWriteOptionAndNoCatalogTable(Class<? extends Sink> sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    Sink sink = mock(sinkType);

    when(writeToMicroBatchV1.sink()).thenReturn(sink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(Collections.singletonMap("path", OUTPUT_PATH)));

    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mockSparkDatasetBuilder();
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
    assertEquals(OUTPUT_PATH, identifierCaptor.getValue().getName());
    assertEquals("file", identifierCaptor.getValue().getNamespace());
    verify(sparkBuilder).schema(schema);
  }

  @ParameterizedTest
  @ValueSource(classes = {DeltaSink.class, FileStreamSink.class})
  void testApply_WithPathSinkPathWriteOptionIsCaseInsensitive(Class<? extends Sink> sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    Sink sink = mock(sinkType);

    when(writeToMicroBatchV1.sink()).thenReturn(sink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(
            ScalaConversionUtils.fromJavaMap(Collections.singletonMap("PaTh", OUTPUT_PATH)));

    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mockSparkDatasetBuilder();
    when(factory.sparkDatasetBuilder()).thenReturn(sparkBuilder);
    when(sparkBuilder.dataset(any(DatasetIdentifier.class))).thenReturn(sparkBuilder);
    when(sparkBuilder.schema(schema)).thenReturn(sparkBuilder);
    when(sparkBuilder.build()).thenReturn(expectedDataset);

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertEquals(1, result.size());
    ArgumentCaptor<DatasetIdentifier> identifierCaptor =
        ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(sparkBuilder).dataset(identifierCaptor.capture());
    assertEquals(OUTPUT_PATH, identifierCaptor.getValue().getName());
  }

  @ParameterizedTest
  @ValueSource(classes = {DeltaSink.class, FileStreamSink.class})
  void testApply_WithPathSinkAndNoPathWriteOption(Class<? extends Sink> sinkType) {
    assertSinkWithoutOutput(sinkType, Collections.emptyMap());
  }

  @ParameterizedTest
  @ValueSource(classes = {DeltaSink.class, FileStreamSink.class})
  void testApply_WithPathSinkAndBlankPathWriteOption(Class<? extends Sink> sinkType) {
    assertSinkWithoutOutput(sinkType, Collections.singletonMap("path", ""));
  }

  private void assertSinkWithoutOutput(
      Class<? extends Sink> sinkType, Map<String, String> writeOptions) {
    when(writeToMicroBatchV1.sink()).thenReturn(mock(sinkType));
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(ScalaConversionUtils.fromJavaMap(writeOptions));

    assertTrue(builder.apply(event, writeToMicroBatchV1).isEmpty());
    verify(factory, never()).sparkDatasetBuilder();
  }

  @SuppressWarnings("unchecked")
  private static DatasetFactory<OpenLineage.OutputDataset> mockDatasetFactory() {
    return mock(DatasetFactory.class);
  }

  @SuppressWarnings("unchecked")
  private static SparkDatasetBuilder<OpenLineage.OutputDataset> mockSparkDatasetBuilder() {
    return mock(SparkDatasetBuilder.class);
  }
}
