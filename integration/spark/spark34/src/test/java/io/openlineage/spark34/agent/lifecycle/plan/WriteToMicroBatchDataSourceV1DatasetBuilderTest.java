/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.PathUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
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
import org.mockito.MockedStatic;
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
  void testApply_WithUnsupportedSink() {
    when(writeToMicroBatchV1.sink()).thenReturn(unsupportedSink);

    List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

    assertTrue(result.isEmpty());
    verify(factory, never()).getDataset(any(DatasetIdentifier.class), any(StructType.class));
  }

  @Test
  void testApply_WithFileStreamSink_ValidPath_NoCatalogTable() {
    String testPath = "file:///tmp/test-output";
    String sinkToString = "FileSink[" + testPath + "]";
    DatasetIdentifier expectedDatasetIdentifier = new DatasetIdentifier("test-output", "file://");
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    try (MockedStatic<PathUtils> pathUtilsMock = mockStatic(PathUtils.class)) {
      pathUtilsMock
          .when(() -> PathUtils.fromURI(URI.create(testPath)))
          .thenReturn(expectedDatasetIdentifier);

      when(factory.getDataset(eq(expectedDatasetIdentifier), eq(schema)))
          .thenReturn(expectedDataset);

      List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

      assertEquals(1, result.size());
      assertEquals(expectedDataset, result.get(0));
      verify(factory).getDataset(eq(expectedDatasetIdentifier), eq(schema));
    }
  }

  @Test
  void testApply_WithFileStreamSink_ValidPath_WithCatalogTable() {
    String testPath = "file:///tmp/test-output";
    String sinkToString = "FileSink[" + testPath + "]";
    DatasetIdentifier pathDatasetIdentifier = new DatasetIdentifier("test-output", "file://");
    DatasetIdentifier catalogDatasetIdentifier =
        new DatasetIdentifier("catalog_table", "catalog_namespace");
    DatasetIdentifier expectedDatasetIdentifier =
        pathDatasetIdentifier.withSymlink(
            catalogDatasetIdentifier.getName(),
            catalogDatasetIdentifier.getNamespace(),
            DatasetIdentifier.SymlinkType.TABLE);

    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    CatalogTable catalogTable = mock(CatalogTable.class);

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    try (MockedStatic<PathUtils> pathUtilsMock = mockStatic(PathUtils.class)) {
      pathUtilsMock
          .when(() -> PathUtils.fromURI(URI.create(testPath)))
          .thenReturn(pathDatasetIdentifier);
      pathUtilsMock
          .when(() -> PathUtils.fromCatalogTable(eq(catalogTable), any()))
          .thenReturn(catalogDatasetIdentifier);

      when(factory.getDataset(eq(expectedDatasetIdentifier), eq(schema)))
          .thenReturn(expectedDataset);

      List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

      assertEquals(1, result.size());
      assertEquals(expectedDataset, result.get(0));
      verify(factory).getDataset(eq(expectedDatasetIdentifier), eq(schema));
    }
  }

  @Test
  void testApply_WithFileStreamSink_CatalogTableException() {
    String testPath = "file:///tmp/test-output";
    String sinkToString = "FileSink[" + testPath + "]";
    DatasetIdentifier pathDatasetIdentifier = new DatasetIdentifier("test-output", "file://");
    CatalogTable catalogTable = mock(CatalogTable.class);

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.apply(catalogTable));
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    try (MockedStatic<PathUtils> pathUtilsMock = mockStatic(PathUtils.class)) {
      pathUtilsMock
          .when(() -> PathUtils.fromURI(URI.create(testPath)))
          .thenReturn(pathDatasetIdentifier);
      pathUtilsMock
          .when(() -> PathUtils.fromCatalogTable(eq(catalogTable), any()))
          .thenThrow(new RuntimeException("Catalog table error"));

      // This should throw the exception since the current implementation doesn't catch it
      assertThrows(RuntimeException.class, () -> builder.apply(event, writeToMicroBatchV1));
    }
  }

  @Test
  void testApply_WithFileStreamSink_InvalidToStringFormat() {
    String invalidSinkToString = "InvalidFormat[/tmp/test]";

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(invalidSinkToString);

    // The current implementation will try to extract from any string, leading to "rmat[/tmp/test"
    // This will cause a URI creation exception
    assertThrows(IllegalArgumentException.class, () -> builder.apply(event, writeToMicroBatchV1));
  }

  @Test
  void testApply_WithFileStreamSink_EmptyPath() {
    String sinkToString = "FileSink[]";
    DatasetIdentifier expectedDatasetIdentifier = new DatasetIdentifier("", "");
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    try (MockedStatic<PathUtils> pathUtilsMock = mockStatic(PathUtils.class)) {
      pathUtilsMock
          .when(() -> PathUtils.fromURI(URI.create("")))
          .thenReturn(expectedDatasetIdentifier);

      when(factory.getDataset(eq(expectedDatasetIdentifier), eq(schema)))
          .thenReturn(expectedDataset);

      // The empty path creates an empty URI which is valid, so no exception is thrown
      List<OpenLineage.OutputDataset> result = builder.apply(event, writeToMicroBatchV1);

      assertEquals(1, result.size());
      assertEquals(expectedDataset, result.get(0));
      verify(factory).getDataset(eq(expectedDatasetIdentifier), eq(schema));
    }
  }

  @Test
  void testApply_WithFileStreamSink_NullToString() {
    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(null);

    // The null toString will cause a NullPointerException
    assertThrows(NullPointerException.class, () -> builder.apply(event, writeToMicroBatchV1));
  }

  @Test
  void testApply_WithFileStreamSink_ToStringThrowsException() {
    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenThrow(new RuntimeException("toString failed"));

    // The exception from toString will propagate
    assertThrows(RuntimeException.class, () -> builder.apply(event, writeToMicroBatchV1));
  }

  @Test
  void testApply_WithDatasetFactoryException() {
    String testPath = "file:///tmp/test-output";
    String sinkToString = "FileSink[" + testPath + "]";
    DatasetIdentifier expectedDatasetIdentifier = new DatasetIdentifier("test-output", "file://");

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    try (MockedStatic<PathUtils> pathUtilsMock = mockStatic(PathUtils.class)) {
      pathUtilsMock
          .when(() -> PathUtils.fromURI(URI.create(testPath)))
          .thenReturn(expectedDatasetIdentifier);

      when(factory.getDataset(eq(expectedDatasetIdentifier), eq(schema)))
          .thenThrow(new RuntimeException("Factory error"));

      // The exception from factory will propagate
      assertThrows(RuntimeException.class, () -> builder.apply(event, writeToMicroBatchV1));
    }
  }

  @Test
  void testApply_WithSchemaException() {
    String testPath = "file:///tmp/test-output";
    String sinkToString = "FileSink[" + testPath + "]";

    when(writeToMicroBatchV1.sink()).thenReturn(fileStreamSink);
    when(writeToMicroBatchV1.schema()).thenThrow(new RuntimeException("Schema error"));
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(fileStreamSink.toString()).thenReturn(sinkToString);

    // The exception from schema will propagate
    assertThrows(RuntimeException.class, () -> builder.apply(event, writeToMicroBatchV1));
  }
}
