/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SparkInputPartitionExtractorTest {
  private SparkInputPartitionExtractor extractor;
  private SparkContext mockSparkContext;
  private TableExtractor mockTableExtractor;
  private Table mockTable;

  @BeforeEach
  void setUp() {
    mockTable = mock(Table.class);
    mockTableExtractor = mock(TableExtractor.class);
    extractor = new SparkInputPartitionExtractor(mockTableExtractor);
    mockSparkContext = mock(SparkContext.class);

    when(mockSparkContext.hadoopConfiguration()).thenReturn(mock(Configuration.class));
    when(mockSparkContext.getConf())
        .thenReturn(
            new SparkConf()
                .set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory")
                .set(StaticSQLConf.WAREHOUSE_PATH().key(), "/tmp/warehouse"));
  }

  @Test
  void returnsTrueForIcebergSparkInputPartition() throws Exception {
    Class<?> sparkInputPartitionClass =
        Class.forName("org.apache.iceberg.spark.source.SparkInputPartition");
    InputPartition inputPartition = (InputPartition) mock(sparkInputPartitionClass);

    assertTrue(extractor.isDefinedAt(inputPartition));
  }

  @Test
  void returnsFalseForFileInputPartition() {
    assertFalse(extractor.isDefinedAt(mock(FilePartition.class)));
  }

  @Test
  void extractReturnsEmptyListWhenTableIsNotPresent() {
    InputPartition mockInputPartition = mock(InputPartition.class);

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.empty());

      List<DatasetIdentifier> result = extractor.extract(mockSparkContext, mockInputPartition);

      assertTrue(result.isEmpty());
    }
  }

  @Test
  void extractReturnsEmptyListWhenTableLocationIsNotPresent() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    when(mockTableExtractor.extractTableLocation(any())).thenReturn(Optional.empty());

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockTable));

      List<DatasetIdentifier> result = extractor.extract(mockSparkContext, mockInputPartition);

      assertTrue(result.isEmpty());
    }
  }

  @Test
  void extractWithLocationOnly() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    when(mockTableExtractor.extractTableLocation(any()))
        .thenReturn(Optional.of(new URI("file:///tmp/source")));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockTable));

      List<DatasetIdentifier> result = extractor.extract(mockSparkContext, mockInputPartition);

      assertEquals(1, result.size());
      assertEquals("file", result.get(0).getNamespace());
      assertEquals("/tmp/source", result.get(0).getName());
      assertEquals(0, result.get(0).getSymlinks().size());
    }
  }

  @Test
  void extractWithLocationAndTableIdentifier() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    when(mockTableExtractor.extractTableLocation(any()))
        .thenReturn(Optional.of(new URI("file:///tmp/warehouse/source")));
    when(mockTableExtractor.extractTableIdentifier(any()))
        .thenReturn(
            Optional.of(
                new TableIdentifier(
                    "source", ScalaConversionUtils.toScalaOption("database_name"))));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockTable));

      List<DatasetIdentifier> result = extractor.extract(mockSparkContext, mockInputPartition);

      assertEquals(1, result.size());
      assertEquals("file", result.get(0).getNamespace());
      assertEquals("/tmp/warehouse/source", result.get(0).getName());

      assertEquals(1, result.get(0).getSymlinks().size());
      assertEquals("database_name.source", result.get(0).getSymlinks().get(0).getName());
      assertEquals("file:/tmp/warehouse", result.get(0).getSymlinks().get(0).getNamespace());
      assertEquals(
          DatasetIdentifier.SymlinkType.TABLE, result.get(0).getSymlinks().get(0).getType());
    }
  }
}
