/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SparkInputPartitionPathExtractorTest {
  private SparkInputPartitionPathExtractor extractor;
  private Configuration mockConfiguration;
  private FileSystem mockFileSystem;

  @BeforeEach
  void setUp() {
    extractor = new SparkInputPartitionPathExtractor();
    mockConfiguration = mock(Configuration.class);
    mockFileSystem = mock(FileSystem.class);
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
  void extractReturnsEmptyListWhenTaskGroupIsNotPresent() {
    InputPartition mockInputPartition = mock(InputPartition.class);

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.empty());

      List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

      assertTrue(result.isEmpty());
    }
  }

  @Test
  void extractReturnsEmptyListWhenTaskGroupIsNotScanTaskGroup() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    Object notATaskGroup = new Object();

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(notATaskGroup));

      List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

      assertTrue(result.isEmpty());
    }
  }

  @Test
  void extractReturnsEmptyListWhenScanTaskGroupIsEmpty() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup<?> mockScanTaskGroup = mock(ScanTaskGroup.class);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.emptyList());

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  void extractFiltersOutNonContentScanTasks() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);

    NonContentScanTask nonContentScanTask = new NonContentScanTask();
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(nonContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  void extractQualifiesPathWithSchemeUnchanged() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("hdfs:///tmp/source/data/file.parquet");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertEquals(1, result.size());
        assertEquals(new URI("hdfs:///tmp/source"), result.get(0).toUri());
      }
    }
  }

  @Test
  void extractQualifiesPathWithoutSchemeUsingLocalFilesystem() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("/tmp/source/data/file.parquet");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic
            .when(() -> FileSystem.get(mockConfiguration))
            .thenReturn(new LocalFileSystem());

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertEquals(1, result.size());
        assertEquals(new URI("file:///tmp/source"), result.get(0).toUri());
      }
    }
  }

  @Test
  void extractQualifiesPathWithoutSchemeUsingDistributedFileSystem()
      throws URISyntaxException, IOException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("/tmp/source/data/file.parquet");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {

        FileSystem distributedFileSystem = new DistributedFileSystem();
        distributedFileSystem.initialize(new URI("hdfs://localhost:8020"), new Configuration());
        fileSystemStatic
            .when(() -> FileSystem.get(mockConfiguration))
            .thenReturn(distributedFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertEquals(1, result.size());
        assertEquals(new URI("hdfs://localhost:8020/tmp/source"), result.get(0).toUri());
      }
    }
  }

  @Test
  void extractReturnsEmptyListWhenPathQualificationFails() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("/tmp/source/data/file.parquet");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    when(mockFileSystem.makeQualified(any(Path.class)))
        .thenThrow(new RuntimeException("Qualification failed"));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  void extractFiltersOutRootPath() {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("/tmp");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertTrue(result.isEmpty());
      }
    }
  }

  @Test
  void extractSkipsDataDirectoryAndReturnsGrandparent() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);
    ContentScanTask mockContentScanTask = mock(ContentScanTask.class);
    DataFile mockDataFile = mock(DataFile.class);

    when(mockDataFile.path()).thenReturn("file:///tmp/source/data/file.parquet");
    when(mockContentScanTask.file()).thenReturn(mockDataFile);
    when(mockScanTaskGroup.tasks()).thenReturn(Collections.singletonList(mockContentScanTask));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertEquals(1, result.size());
        assertEquals(new URI("file:///tmp/source"), result.get(0).toUri());
      }
    }
  }

  @Test
  void extractProcessesMultipleContentScanTasks() throws URISyntaxException {
    InputPartition mockInputPartition = mock(InputPartition.class);
    ScanTaskGroup mockScanTaskGroup = mock(ScanTaskGroup.class);

    ContentScanTask mockContentScanTask1 = mock(ContentScanTask.class);
    DataFile mockDataFile1 = mock(DataFile.class);
    when(mockDataFile1.path()).thenReturn("file:///tmp/source1/data/file1.parquet");
    URI expectedUri1 = new URI("file:///tmp/source1");
    when(mockContentScanTask1.file()).thenReturn(mockDataFile1);

    ContentScanTask mockContentScanTask2 = mock(ContentScanTask.class);
    DataFile mockDataFile2 = mock(DataFile.class);
    when(mockDataFile2.path()).thenReturn("file:///tmp/source2/data/file2.parquet");
    URI expectedUri2 = new URI("file:///tmp/source2");
    when(mockContentScanTask2.file()).thenReturn(mockDataFile2);

    when(mockScanTaskGroup.tasks())
        .thenReturn(Arrays.asList(mockContentScanTask1, mockContentScanTask2));

    try (MockedStatic<?> reflectionUtils =
        mockStatic(io.openlineage.spark.agent.util.ReflectionUtils.class)) {
      reflectionUtils
          .when(
              () -> io.openlineage.spark.agent.util.ReflectionUtils.tryExecuteMethod(any(), any()))
          .thenReturn(Optional.of(mockScanTaskGroup));

      try (MockedStatic<FileSystem> fileSystemStatic = mockStatic(FileSystem.class)) {
        fileSystemStatic.when(() -> FileSystem.get(mockConfiguration)).thenReturn(mockFileSystem);

        List<Path> result = extractor.extract(mockConfiguration, mockInputPartition);

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(p -> p.toUri().equals(expectedUri1)));
        assertTrue(result.stream().anyMatch(p -> p.toUri().equals(expectedUri2)));
      }
    }
  }

  private static class NonContentScanTask implements ScanTask {}
}
