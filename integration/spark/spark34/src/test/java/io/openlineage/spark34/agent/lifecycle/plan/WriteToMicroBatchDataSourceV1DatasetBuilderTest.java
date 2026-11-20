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
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.internal.SessionState;
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
  private static final String SINK = "org.apache.spark.sql.execution.streaming.Sink";
  private static final String DELTA_SINK = "org.apache.spark.sql.delta.sources.DeltaSink";
  private static final String FILE_STREAM_SINK =
      "org.apache.spark.sql.execution.streaming.FileStreamSink";
  private static final String RELOCATED_FILE_STREAM_SINK =
      "org.apache.spark.sql.execution.streaming.sinks.FileStreamSink";
  private static final String REMOTE_FS = "remotefs://cluster";

  private DatasetFactory<OpenLineage.OutputDataset> factory;
  private WriteToMicroBatchDataSourceV1DatasetBuilder builder;
  private WriteToMicroBatchDataSourceV1 writeToMicroBatchV1;
  private Sink unsupportedSink;
  private SparkListenerSQLExecutionEnd event;
  private SessionState sessionState;
  private StructType schema;

  @BeforeEach
  void setUp() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    SparkSession sparkSession = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    sessionState = mock(SessionState.class);
    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.newHadoopConf()).thenReturn(new Configuration());

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
  @ValueSource(strings = {SINK, DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithCatalogTableRegardlessOfSink(String sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    CatalogTable catalogTable = mock(CatalogTable.class);

    when(writeToMicroBatchV1.sink()).thenReturn(mockSink(sinkType));
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
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithPathSinkPathWriteOptionAndNoCatalogTable(String sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    Sink sink = mockSink(sinkType);

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
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithPathSinkPathWriteOptionIsCaseInsensitive(String sinkType) {
    OpenLineage.OutputDataset expectedDataset = mock(OpenLineage.OutputDataset.class);
    Sink sink = mockSink(sinkType);

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
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithPathSinkAndNoPathWriteOption(String sinkType) {
    assertSinkWithoutOutput(sinkType, Collections.emptyMap());
  }

  @ParameterizedTest
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithPathSinkAndBlankPathWriteOption(String sinkType) {
    assertSinkWithoutOutput(sinkType, Collections.singletonMap("path", ""));
  }

  @ParameterizedTest
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithRelativePathIsQualifiedAgainstWorkingDirectory(String sinkType) {
    DatasetIdentifier identifier =
        applyPathSinkWrite(sinkType, Collections.singletonMap("path", "relative_output"));

    assertEquals("file", identifier.getNamespace());
    assertEquals(new File("relative_output").getAbsolutePath(), identifier.getName());
  }

  @ParameterizedTest
  @ValueSource(strings = {DELTA_SINK, FILE_STREAM_SINK, RELOCATED_FILE_STREAM_SINK})
  void testApply_WithPathQualifiedAgainstSessionDefaultFsIgnoringWriteOptions(String sinkType) {
    when(sessionState.newHadoopConf()).thenReturn(remoteHadoopConf(true));
    // The sinks resolve their path without write options, so this override must not apply.
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put("path", "relative_output");
    writeOptions.put("fs.defaultFS", "file:///");

    DatasetIdentifier identifier = applyPathSinkWrite(sinkType, writeOptions);

    assertEquals(REMOTE_FS, identifier.getNamespace());
    assertEquals("/user/test/relative_output", identifier.getName());
    verify(sessionState, never()).newHadoopConfWithOptions(any());
  }

  @Test
  void testApply_ClosesFileSystemCreatedOutsideCache() {
    when(sessionState.newHadoopConf()).thenReturn(remoteHadoopConf(true));
    RemoteFileSystem.CLOSED.set(0);

    applyPathSinkWrite(FILE_STREAM_SINK, Collections.singletonMap("path", "/data/output"));

    assertEquals(1, RemoteFileSystem.CLOSED.get());
  }

  @Test
  void testApply_KeepsCachedFileSystemOpen() throws IOException {
    Configuration hadoopConf = remoteHadoopConf(false);
    when(sessionState.newHadoopConf()).thenReturn(hadoopConf);
    RemoteFileSystem.CLOSED.set(0);

    try {
      applyPathSinkWrite(FILE_STREAM_SINK, Collections.singletonMap("path", "/data/output"));

      assertEquals(0, RemoteFileSystem.CLOSED.get());
    } finally {
      FileSystem.get(URI.create(REMOTE_FS), hadoopConf).close();
    }
  }

  @Test
  void testApply_WithUnqualifiablePathFallsBackToRawPath() {
    when(sessionState.newHadoopConf()).thenThrow(new IllegalStateException("no conf"));

    DatasetIdentifier identifier =
        applyPathSinkWrite(FILE_STREAM_SINK, Collections.singletonMap("path", "relative_output"));

    assertEquals("file", identifier.getNamespace());
    assertEquals("relative_output", identifier.getName());
  }

  private DatasetIdentifier applyPathSinkWrite(String sinkType, Map<String, String> writeOptions) {
    when(writeToMicroBatchV1.sink()).thenReturn(mockSink(sinkType));
    when(writeToMicroBatchV1.schema()).thenReturn(schema);
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(ScalaConversionUtils.fromJavaMap(writeOptions));

    SparkDatasetBuilder<OpenLineage.OutputDataset> sparkBuilder = mockSparkDatasetBuilder();
    when(factory.sparkDatasetBuilder()).thenReturn(sparkBuilder);
    when(sparkBuilder.dataset(any(DatasetIdentifier.class))).thenReturn(sparkBuilder);
    when(sparkBuilder.schema(schema)).thenReturn(sparkBuilder);
    when(sparkBuilder.build()).thenReturn(mock(OpenLineage.OutputDataset.class));

    assertEquals(1, builder.apply(event, writeToMicroBatchV1).size());
    ArgumentCaptor<DatasetIdentifier> identifierCaptor =
        ArgumentCaptor.forClass(DatasetIdentifier.class);
    verify(sparkBuilder).dataset(identifierCaptor.capture());
    return identifierCaptor.getValue();
  }

  private void assertSinkWithoutOutput(String sinkType, Map<String, String> writeOptions) {
    when(writeToMicroBatchV1.sink()).thenReturn(mockSink(sinkType));
    when(writeToMicroBatchV1.catalogTable()).thenReturn(Option.empty());
    when(writeToMicroBatchV1.writeOptions())
        .thenReturn(ScalaConversionUtils.fromJavaMap(writeOptions));

    assertTrue(builder.apply(event, writeToMicroBatchV1).isEmpty());
    verify(factory, never()).sparkDatasetBuilder();
  }

  private static Configuration remoteHadoopConf(boolean disableCache) {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", REMOTE_FS);
    hadoopConf.setClass("fs.remotefs.impl", RemoteFileSystem.class, RawLocalFileSystem.class);
    hadoopConf.setBoolean("fs.remotefs.impl.disable.cache", disableCache);
    return hadoopConf;
  }

  /** Non-local filesystem that qualifies paths against its own URI without touching storage. */
  public static class RemoteFileSystem extends RawLocalFileSystem {
    private static final URI REMOTE_URI = URI.create(REMOTE_FS);
    static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public URI getUri() {
      return REMOTE_URI;
    }

    @Override
    public Path getWorkingDirectory() {
      return new Path(REMOTE_FS + "/user/test");
    }

    @Override
    public void close() throws IOException {
      CLOSED.incrementAndGet();
      super.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static Sink mockSink(String className) {
    try {
      return mock((Class<? extends Sink>) Class.forName(className));
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
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
